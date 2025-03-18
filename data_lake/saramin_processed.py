import logging
import sys
import boto3
from pyspark.sql.functions import col, explode, from_unixtime, regexp_replace
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 설정
S3_BUCKET = "saramin-data-bucket"
AWS_REGION = "ap-northeast-2"

# s3_path 받아오기
args = getResolvedOptions(sys.argv, ['s3_path', 'target_date', 'JOB_NAME'])
s3_path = args['s3_path']  # Airflow에서 받은 s3_path
target_date = args['target_date'] 

# GlueContext 사용해서 SparkSession 생성
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

logger.info(f"Received s3_path from Airflow: {s3_path}")
logger.info(f"Received target_date from Airflow: {target_date}")

def main():
    """S3에서 JSON 데이터를 가져와 Spark로 처리 후 Parquet 저장"""
    
    # S3에서 JSON 데이터 읽기
    s3_uri = f"s3://{S3_BUCKET}/{s3_path}"
    
    df = spark.read.option("multiline", "true").json(s3_uri)

    # job 필드 explode 처리
    jobs_df = df.select(explode(col("jobs.job")).alias("job"))

    # 필터링 (예시: 특정 조건을 만족하는 데이터만 유지)
    filtered_df = jobs_df.filter(
        (col("job.position.job-mid-code.code").contains("2")) &  # "2"가 포함된 공고만 유지
        col("job.position.industry.code").like("3%")   # "3"으로 시작하는 공고만 유지
    )

    # 필요한 컬럼 및 컬럼 이름 지정
    renamed_df = (filtered_df
        .withColumn("url", col("job.url"))
        .withColumn("keyword", col("job.keyword"))
        .withColumn("id", col("job.id"))
        .withColumn("posting_date", col("job.posting-date"))
        .withColumn("opening_date", from_unixtime(col("job.opening-timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"))  # timestamp 변환
        .withColumn("expiration_date", col("job.expiration-date"))
        .withColumn("read_cnt", col("job.read-cnt"))
        .withColumn("apply_cnt", col("job.apply-cnt"))
        .withColumn("company_name", col("job.company.detail.name"))
        .withColumn("company_url", col("job.company.detail.href"))
        .withColumn("title", col("job.position.title"))
        .withColumn("industry_code", col("job.position.industry.code"))
        .withColumn("industry_name", col("job.position.industry.name"))
        .withColumn("location_code", col("job.position.location.code"))
        .withColumn("location_name", regexp_replace(col("job.position.location.name"), "&gt;", ">"))  # '&gt;'를 '>'로 변환
        .withColumn("job_type_code", col("job.position.job-type.code"))
        .withColumn("job_type_name", col("job.position.job-type.name"))
        .withColumn("job_mid_code", col("job.position.job-mid-code.code"))
        .withColumn("job_mid_name", col("job.position.job-mid-code.name"))
        .withColumn("job_code", col("job.position.job-code.code"))
        .withColumn("job_name", col("job.position.job-code.name"))
        .withColumn("experience_level_code", col("job.position.experience-level.code"))
        .withColumn("experience_level_min", col("job.position.experience-level.min"))
        .withColumn("experience_level_max", col("job.position.experience-level.max"))
        .withColumn("experience_level_name", col("job.position.experience-level.name"))
        .withColumn("required_education_level_code", col("job.position.required-education-level.code"))
        .withColumn("required_education_level_name", col("job.position.required-education-level.name"))
        .withColumn("salary_code", col("job.salary.code"))
        .withColumn("salary_name", col("job.salary.name"))
        .withColumn("close_type_code", col("job.close-type.code"))
        .withColumn("close_type_name", col("job.close-type.name"))
        .drop("job")  # 기존 중첩된 "job" 컬럼 삭제
    )

    s3_client = boto3.client('s3', region_name=AWS_REGION)
    S3_FOLDER = f"{target_date[:4]}/{target_date[4:6]}/{target_date[6:8]}/"
    server_time = f"{target_date[:4]}_{target_date[4:6]}_{target_date[6:8]}_{int(target_date[8:10])+9}h"

    # 최종 파일명
    filename = f"saramin_process_data_{server_time}.parquet"

    # 임시 저장 경로 설정 -> 임시 저장 할 필요는 없지만 한 폴더에 쌓아두기 위해 임시로 저장
    s3_temp_path = f"s3://{S3_BUCKET}/saramin/process_data/{S3_FOLDER}temp/"

    # 임시 폴더에 저장
    renamed_df.write.mode("overwrite").parquet(s3_temp_path)

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
        s3_client.delete_object(Bucket=S3_BUCKET, Key=parquet_file_key)
        logger.info("[INFO] Temp file deleted.")

    logger.info(f"Uploaded Parquet to S3: {new_filename}")

if __name__ == "__main__":
    main()