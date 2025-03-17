from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from saramin_get_data import fetch_saramin_job_data
from datetime import datetime,timedelta

# Airflow DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 11),
    "retries": 1, # task 실패시 1번 재시도
    "retry_delay": timedelta(minutes=5), # 5분 후 재시도
}

@dag(
    dag_id="saramin_project_dags",
    default_args=default_args,
    description="Saramin 채용공고 원본 JSON저장 및 Parquet 변환",
    schedule_interval="0 10-20/2 * * *",  # UTC 기준: 01시~11시, 2시간 간격 실행 / 한국 시간 기준: 10시~20시
    catchup=False, # 과거의 미실행된 DAG는 실행하지 않음 (현재 주기부터 실행됨)
)

def saramin_dag():
    # Airflow에서 Saramin API 호출 → S3에 JSON 저장
    @task
    def fetch_data(data_interval_end):
        return fetch_saramin_job_data(target_date=data_interval_end)
    
    s3_path = fetch_data()

    process_data = GlueJobOperator(
        task_id="process_saramin_job_data",
        job_name="saramin-glue-job", 
        region_name="ap-northeast-2",
        script_args={
            "--s3_path": s3_path, 
            "--target_date": "{{ data_interval_end.strftime('%Y%m%d%H') }}"
        },
    )
    s3_path >> process_data

saramin_dag()