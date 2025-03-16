from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 11, 10, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Saramin_etl_dag',
    default_args=default_args,
    schedule_interval='0 10-20/2 * * *', 
    catchup=False,
    description='Airflow DAG that triggers an AWS Glue job to process the latest update from the daily folder',
) as dag:

    run_glue_job = GlueJobOperator(
        task_id='run_glue_etl_job',
        job_name='analytics-glue-job',
        script_location='s3://saramin-glue-code/etl_process_glue.py',
        region_name='ap-northeast-2',
        iam_role_name='Saramin-Glue-S3-AccessRole',
	create_job_kwargs={
		"GlueVersion": "3.0",
		"NumberOfWorkers": 10,
		"WorkerType": "G.1X",
	},
	script_args={
		'--s3_folder': "{{ execution_date.strftime('s3://saramin-data-bucket/saramin/process_data/%Y/%m/%d/') }}"
	},
    )
    run_glue_job

