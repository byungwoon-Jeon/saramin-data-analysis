import logging
import sys
from pyspark.sql.functions import col, explode, lit, from_unixtime, regexp_replace
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 설정
S3_BUCKET = "saramin-data-bucket"

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
    
    # target_date를 분리하여 새로운 컬럼 추가
    partitioned_df = (renamed_df
        .withColumn("year", lit(target_date[:4]))    # 연도
        .withColumn("month", lit(target_date[4:6]))  # 월
        .withColumn("day", lit(target_date[6:8]))    # 일
        .withColumn("hour", lit(target_date[8:10]))  # 시간
    )

    # 저장 경로
    s3_parquet_path = f"s3://{S3_BUCKET}/saramin/process_data/"

    # .partitionBy() 사용하여 파티션 저장 + 동적 파티션 업데이트 옵션 추가
    partitioned_df.write \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("year", "month", "day", "hour") \
        .parquet(s3_parquet_path)

    logger.info(f"Uploaded Parquet to S3: {s3_parquet_path}")


if __name__ == "__main__":
    main()
