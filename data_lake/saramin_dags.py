from airflow import DAG
from airflow.operators.python import PythonOperator
from saramin_get_data import fetch_saramin_job_data
from datetime import datetime,timedelta
import boto3

# AWS Glue Job 실행 함수
def trigger_glue_job(**kwargs):
    """AWS Glue Job 실행"""
    try:
        client = boto3.client("glue", region_name="ap-northeast-2")  # Glue 클라이언트 생성
        s3_path = kwargs['ti'].xcom_pull(task_ids='fetch_saramin_job_data')  # XCom으로 s3_path 가져오기
        data_interval_end = kwargs["data_interval_end"]

        if not s3_path:
            raise ValueError("s3_path를 가져오지 못했습니다.")
        
        # Glue에 전달할 수 있도록 data_interval_end를 문자열로 변환
        data_interval_end_str = data_interval_end.strftime("%Y-%m-%d %H:%M:%S%z")
        
        response = client.start_job_run( # Glue job 실행
            JobName="saramin-glue-job",
            Arguments={
                "--s3_path": s3_path,  
                "--data_interval_end": data_interval_end_str  # Glue Job에 전달
            }
        )
        job_run_id = response["JobRunId"]
        print(f"Glue Job Started: {job_run_id}")  # 실행된 Glue Job의 ID 출력
    except Exception as e:
        print(f"Glue Job 실행 실패: {str(e)}")

# Airflow DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 11),
    "retries": 1, # task 실패시 1번 재시도
    "retry_delay": timedelta(minutes=5), # 5분 후 재시도
}

dag = DAG(
    dag_id="saramin_project_dags",
    default_args=default_args,
    description="Saramin 채용공고 원본 JSON저장 및 Parquet 변환",
    schedule_interval="0 9-21/2 * * *",  # UTC 기준: 00시~12시, 2시간 간격 실행 / 한국 시간 기준: 09시~21시
    catchup=False, # 과거의 미실행된 DAG는 실행하지 않음 (현재 주기부터 실행됨)
)

# Airflow에서 Saramin API 호출 → S3에 JSON 저장
fetch_saramin_job_data = PythonOperator(
    task_id="fetch_saramin_job_data",
    python_callable=fetch_saramin_job_data,
    op_kwargs={"data_interval_end": "{{ data_interval_end }}"},
    dag=dag,
)

# Glue에서 S3 JSON → Parquet 변환 실행
process_saramin_job_data = PythonOperator(
    task_id="process_saramin_job_data",
    python_callable=trigger_glue_job,  # boto3를 사용하여 Glue 실행
    dag=dag,
)

fetch_saramin_job_data >> process_saramin_job_data