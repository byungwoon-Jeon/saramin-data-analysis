from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 12, 10, 0),  # 첫 실행은 오전 10시로 설정
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='glue_etl_latest_data_per_update',
    default_args=default_args,
    schedule_interval='0 10-20/2 * * *',  # 오전 10시부터 오후 8시까지 2시간 간격
    catchup=False,
    description='Airflow DAG that triggers an AWS Glue job to process the latest update from the daily folder',
) as dag:

    # 실행 시점의 날짜를 이용해 S3의 해당 날짜 폴더 경로를 동적으로 구성합니다.
    # 예: 실행일이 2025-03-12이면 "s3://saramin-data-bucket/saramin/process_data/2025/03/12/" 경로가 생성됩니다.
    s3_folder = "{{ execution_date.strftime('s3://saramin-data-bucket/saramin/process_data/%Y/%m/%d/') }}"

    run_glue_job = AwsGlueJobOperator(
        task_id='run_glue_etl_job',
        job_name='your_glue_job_name',  # Glue 콘솔에 등록된 Glue Job 이름으로 변경하세요.
        script_location='s3://your-script-bucket/path/to/glue_script.py',  # Glue 스크립트 경로
        region_name='ap-northeast-2',  # 예시: 서울 리전
        iam_role_name='your-iam-role',  # Glue Job 실행에 필요한 IAM 역할
        number_of_dpus=10,
        timeout=60,
        # Glue 스크립트에 '--s3_folder' 파라미터로 동적으로 구성된 경로 전달
        job_args={'--s3_folder': s3_folder},
    )

    run_glue_job

