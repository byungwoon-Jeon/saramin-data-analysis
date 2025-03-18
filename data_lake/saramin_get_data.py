from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests
import logging
import json
import time
import pendulum

API_KEY = Variable.get("saramin_api_key")
BASE_URL = "https://oapi.saramin.co.kr/job-search"
S3_BUCKET = "saramin-data-bucket"
KST = pendulum.timezone('Asia/Seoul')

def fetch_saramin_job_data(target_date) -> str:
    """
    API 호출하여 데이터를 json 파일로 저장
    """

    # 한국 시간으로 변환
    target_date_kst = target_date.astimezone(KST)

    file_name = f"saramin_raw_data_{target_date_kst.strftime('%Y_%m_%d_%Hh')}.json"
    s3_path = f"saramin/raw_data/{target_date_kst.strftime('%Y/%m/%d/')}{file_name}"

    params = {
        "access-key": API_KEY,
        "count": 110,
        "fields": "posting-date,expiration-date,keyword-code,count",
        "ind_cd": 3,
        "job_mid_cd": 2,
    }

    # 전체 데이터 저장 리스트
    all_jobs = []

    response = requests.get(BASE_URL, params=params)
    if response.status_code != 200:
        raise Exception(f"API 호출 실패: {response.status_code} - {response.text}")

    jobs = response.json().get("jobs", {})
    if not jobs:
        raise ValueError("API 응답에 데이터가 없습니다.")
    
    all_jobs.extend(jobs.get("job", []))
    total = int(jobs.get("total", 0))
    logging.info(f"총 공고 수: {total}")

    # 총 페이지 계산
    total_page = (total // 110) + (1 if total % 110 > 0 else 0)

    # 반복해서 데이터 수집
    for page in range(1, total_page+1):
        params["start"] = page
        response = requests.get(BASE_URL, params=params)
        
        if response.status_code == 200:
            jobs = response.json().get("jobs", {})
            all_jobs.extend(jobs.get("job", []))
        else:
            logging.warning(f"{page}page API 호출 실패: {response.status_code}")
        
        time.sleep(1)

    # JSON 파일로 저장
    json_data = json.dumps({"jobs": {"total": total, "job": all_jobs}}, indent=4, ensure_ascii=False)

    # S3에 업로드
    try:
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_string(
            string_data=json_data,
            bucket_name=S3_BUCKET,
            key=s3_path,
            replace=True  # 동일한 키에 덮어쓸지 여부
        )
        logging.info(f"S3 업로드 완료: s3://{S3_BUCKET}/{s3_path}")
    except Exception as e:
        logging.error(f"S3 업로드 실패: {e}")
        raise
    
    # 이 주소를 바로 다음 task로 return
    return s3_path
