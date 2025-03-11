import boto3
import logging
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_unixtime
from awsglue.context import GlueContext
from awsglue.transforms import *
from datetime import datetime, timedelta

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 설정
S3_BUCKET = "saramin-data-bucket"
AWS_REGION = "ap-northeast-2"

def get_latest_s3_json_file(s3_client, execution_date):
    """원본 JSON파일이 저장된 S3 버킷에서 가장 최근 JSON파일 가져오기"""
    # 한국 시간 맞추기 UTC+9 (Glue에서 pendulum 사용 시 오류 발생)
    execution_date_kst = execution_date + timedelta(hours=9)
    S3_FOLDER = execution_date_kst.strftime('%Y/%m/%d/')
    prefix = f"saramin/raw_data/{S3_FOLDER}"
    
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    if 'Contents' not in response:
        logger.warning("No files found in S3.")
        return None

    # 최신 파일 선택
    latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
    logger.info(f"Latest JSON file: {latest_file}")
    return latest_file

def process_saramin_job_data():
    """S3에서 JSON 데이터를 가져와 Spark로 처리 후 Parquet 저장"""

    # Spark 생성
    spark = SparkSession.builder \
        .appName("SaraminJobProcessing") \
        .getOrCreate()

    glueContext = GlueContext(spark.sparkContext)
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    
    # 실행 날짜 (현재 날짜 기준)
    execution_date = datetime.now()
    
    # 최신 JSON 파일 가져오기
    latest_json_file = get_latest_s3_json_file(s3_client, execution_date)
    if not latest_json_file:
        logger.warning("No JSON files found for processing.")
        return
    
    # S3에서 JSON 데이터 읽기
    s3_uri = f"s3://{S3_BUCKET}/{latest_json_file}"
    df = spark.read.option("multiline", "true").json(s3_uri)

    # job 필드 explode 처리
    df_jobs = df.select(explode(col("jobs.job")).alias("job"))

    # 필터링
    df_filtered = df_jobs.filter(
        (col("job.position.job-mid-code.code").contains("2")) &  # "2"가 포함된 공고만 유지
        col("job.position.industry.code").like("3%")   # "3"으로 시작하는 공고만 유지
    )

    # 필요한 컬럼 및 컬럼 이름 지정
    columns = {
        "job.url": "job_url",
        "job.keyword": "keyword",
        "job.id": "id",
        "job.posting-date": "posting_date",
        "job.opening-timestamp": "opening_date",
        "job.expiration-date": "expiration_date",
        "job.read-cnt": "read_cnt",
        "job.apply-cnt": "apply_cnt",
        "job.company.detail.name": "company_name",
        "job.company.detail.href": "company_url",
        "job.position.title": "title",
        "job.position.industry.code": "industry_code",
        "job.position.industry.name": "industry_name",
        "job.position.location.code": "location_code",
        "job.position.location.name": "location_name",
        "job.position.job-type.code": "job_type_code",
        "job.position.job-type.name": "job_type_name",
        "job.position.job-mid-code.code": "job_mid_code",
        "job.position.job-mid-code.name": "job_mid_name",
        "job.position.job-code.code": "job_code",
        "job.position.job-code.name": "job_name",
        "job.position.experience-level.code": "experience_level_code",
        "job.position.experience-level.min": "experience_level_min",
        "job.position.experience-level.max": "experience_level_max",
        "job.position.experience-level.name": "experience_level_name",
        "job.position.required-education-level.code": "required_education_level_code",
        "job.position.required-education-level.name": "required_education_level_name",
        "job.salary.code": "salary_code",
        "job.salary.name": "salary_name",
        "job.close-type.code": "close_type_code",
        "job.close-type.name": "close_type_name"
    }

    # Unix timestamp을 날짜 형식으로 변환, 컬럼 정리
    df_cleaned = df_filtered.select([
        from_unixtime(col(src), "yyyy-MM-dd'T'HH:mm:ssXXX").alias(dest) if src == "job.opening-timestamp"
        else col(src).alias(dest)
        for src, dest in columns.items()
    ])

    # S3 저장 경로 설정
    execution_date_kst = execution_date + timedelta(hours=9)
    S3_FOLDER = execution_date_kst.strftime('%Y/%m/%d/')
    server_time = execution_date_kst.strftime("%Y_%m_%d_%Hh")

    # 최종 파일명
    filename = f"saramin_process_data_{server_time}.parquet"

    # 임시 저장 경로 설정
    s3_temp_path = f"s3://{S3_BUCKET}/saramin/process_data/{S3_FOLDER}temp/"

    # 임시 폴더에 저장
    df_cleaned.write.mode("overwrite").parquet(s3_temp_path)

    # 임시 저장된 파일 찾기
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"saramin/process_data/{S3_FOLDER}temp/")

    parquet_file_key = None
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".parquet"):
                parquet_file_key = obj["Key"]
                break

    if parquet_file_key:
        # 최종 경로로 이동 및 이름 변경
        new_filename = f"saramin/process_data/{S3_FOLDER}{filename}"
        s3_client.copy_object(
            Bucket=S3_BUCKET,
            CopySource={'Bucket': S3_BUCKET, 'Key': parquet_file_key},
            Key=new_filename
        )
        logger.info(f"[SUCCESS] Renamed file to {new_filename}")

        # 임시 폴더 삭제 (part-xxxx.parqeut파일도 같이 삭제)
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"saramin/process_data/{S3_FOLDER}temp/")
        if "Contents" in response:
            for obj in response["Contents"]:
                s3_client.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
            logger.info("[INFO] Temp folder cleaned up.")

    logger.info(f"Uploaded Parquet to S3: {new_filename}")

if __name__ == "__main__":
    process_saramin_job_data()
