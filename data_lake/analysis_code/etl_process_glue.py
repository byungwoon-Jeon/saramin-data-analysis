import sys
import boto3
import logging
import re
from datetime import datetime, timedelta

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, split, explode, udf
from pyspark.sql.types import StringType

from awsglue.context import GlueContext
from awsglue.transforms import *

# 로깅 설정
logger = logging.getLogger('GlueJob')
logger.setLevel(logging.INFO)

def get_latest_parquet(s3_folder, bucket_name):
    """
    지정된 S3 폴더 내의 Parquet 파일들 중 LastModified 기준으로 최신 파일의 경로를 반환
    """
    s3_client = boto3.client('s3')
    prefix = s3_folder.replace(f"s3://{bucket_name}/", "")
    logger.info(f"Listing objects in bucket '{bucket_name}' with prefix '{prefix}'")
    
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get('Contents', [])
    
    parquet_files = [obj for obj in objects if obj['Key'].endswith('.parquet')]
    if not parquet_files:
        raise Exception(f"No Parquet files found in {s3_folder}")
    
    latest_obj = max(parquet_files, key=lambda obj: obj['LastModified'])
    latest_file_path = f"s3://{bucket_name}/{latest_obj['Key']}"
    logger.info(f"Latest Parquet file selected: {latest_file_path}")
    return latest_file_path

def get_analysis_output_path(s3_folder, bucket_name):
    """
    분석 데이터를 저장할 경로를 's3://saramin-data-bucket/saramin/analysis_data/MM-DD/' 형식으로 반환
    """
    pattern = re.compile(r'/(\d{4})/(\d{2})/(\d{2})/')
    match = pattern.search(s3_folder)
    if match:
        year, month, day = match.groups()
        analysis_path = f"s3://{bucket_name}/saramin/analysis_data/{month}-{day}/"
        logger.info(f"Analysis output path set to: {analysis_path}")
        return analysis_path
    else:
        raise ValueError("s3_folder 형식이 올바르지 않습니다. 예: s3://.../YYYY/MM/DD/")

# UDF: location_name 변환 함수
def transform_location(loc):
    """
    loc 값이 "서울 > 서울전체"이면 "서울 전체"로, 
    "서울 > 강서구"이면 "서울시 강서구"로 변환
    """
    if loc is None:
        return None
    parts = loc.split(">")
    if len(parts) == 2:
        city = parts[0].strip()
        district = parts[1].strip()
        if district == "서울전체":
            return f"{city} 전체"
        else:
            # city에 "시"가 없으면 붙여서 출력
            if not city.endswith("시"):
                city = f"{city}시"
            return f"{city} {district}"
    else:
        return loc

transform_location_udf = udf(transform_location, StringType())

def main():
    # Spark 및 Glue Context 생성
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Glue Job 인자 파싱 
    args = sys.argv[1:]
    s3_folder = None
    bucket_name = "saramin-data-bucket"
    for i in range(len(args)):
        if args[i] == '--s3_folder':
            s3_folder = args[i+1]
            break

    if not s3_folder:
        raise ValueError("The '--s3_folder' parameter is required.")

    logger.info(f"Processing S3 folder: {s3_folder}")

    latest_file = get_latest_parquet(s3_folder, bucket_name)

    # 최신 파일 읽기
    logger.info(f"Reading data from: {latest_file}")
    df = spark.read.parquet(latest_file)

    # 날짜 컬럼 변환
    df = df.withColumn(
        "posting_date",
        date_format(to_timestamp(col("posting_date"), "yyyy-MM-dd'T'HH:mm:ssXXX"), "yyyy-MM-dd")
    ).withColumn(
        "opening_date",
        date_format(to_timestamp(col("opening_date"), "yyyy-MM-dd'T'HH:mm:ssXXX"), "yyyy-MM-dd")
    ).withColumn(
        "expiration_date",
        date_format(to_timestamp(col("expiration_date"), "yyyy-MM-dd'T'HH:mm:ssXXX"), "yyyy-MM-dd")
    )

    # 삭제할 컬럼 목록
    columns_to_drop = [
        "company_url", "industry_code", "location_code", "job_type_code",
        "job_mid_code", "job_code", "experience_level_code", "experience_level_min",
        "experience_level_max", "required_education_level_code", "salary_code", "close_type_code"
    ]
    df = df.drop(*columns_to_drop)

    # job_name 컬럼을 콤마 기준으로 배열로 분리한 후 explode하여 각 값(job_name_single)으로 확장
    df = df.withColumn("job_name_array", split(col("job_name"), ",")) \
           .withColumn("job_name_single", explode(col("job_name_array")))
    
    # location_name 컬럼도 콤마 기준으로 배열로 분리한 후 explode하여 각 값(location_name_single)으로 확장
    df = df.withColumn("location_name_array", split(col("location_name"), ",")) \
           .withColumn("location_name_single", explode(col("location_name_array")))
    
    # location_name_single 값 변환
    df = df.withColumn("location_name_single", transform_location_udf(col("location_name_single")))
    
    # 분석용 데이터를 저장할 경로
    output_path = get_analysis_output_path(s3_folder, bucket_name)
    logger.info(f"Writing processed data to: {output_path}")

    # 처리 결과를 Parquet 형식으로 저장
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Glue Job completed successfully.")
    
    sc.stop()

if __name__ == "__main__":
    main()

