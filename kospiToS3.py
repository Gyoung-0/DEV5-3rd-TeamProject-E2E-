from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import boto3
import xml.etree.ElementTree as ET
import os

# Airflow 환경 변수에서 가져오기
KOPIS_API_KEY = Variable.get("KOPIS_API_KEY")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_FOLDER_PATH = Variable.get("S3_FOLDER_PATH")  # "kopis_daily/"

# KOPIS API URL
BASE_URL = "http://kopis.or.kr/openApi/restful/prfstsTotal"

# AWS S3 클라이언트 설정
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# API 호출 및 JSON 저장 함수
def fetch_kopis_data(**context):
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

    params = {
        "service": KOPIS_API_KEY,
        "ststype": "day",
        "stdate": yesterday,
        "eddate": yesterday
    }

    response = requests.get(BASE_URL, params=params)

    # 응답 상태 코드 확인
    if response.status_code != 200:
        raise Exception(f"API 요청 실패! HTTP 상태 코드: {response.status_code}\n응답 내용: {response.text}")

    try:
        # XML 응답을 JSON으로 변환
        root = ET.fromstring(response.text)
        performances = []

        for prfst in root.findall("prfst"):
            performance_data = {
                "prfcnt": int(prfst.find("prfcnt").text),
                "ntssnmrs": int(prfst.find("ntssnmrs").text),
                "cancelnmrs": int(prfst.find("cancelnmrs").text),
                "amount": int(prfst.find("amount").text),
                "nmrs": int(prfst.find("nmrs").text),
                "prfdtcnt": int(prfst.find("prfdtcnt").text),
                "prfdt": prfst.find("prfdt").text,  # 날짜 그대로 저장
                "prfprocnt": int(prfst.find("prfprocnt").text)
            }
            performances.append(performance_data)

    except Exception as e:
        raise Exception(f"XML 파싱 실패: {e}\n응답 내용: {response.text}")

    # JSON 파일 저장 (날짜 포함)
    json_filename = f"kopis_performance_{yesterday}.json"
    file_path = os.path.join("/tmp", json_filename)

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(performances, file, indent=4, ensure_ascii=False)

    print(f"JSON 데이터가 저장되었습니다: {file_path}")

    # XCom을 통해 파일 경로 반환
    context["ti"].xcom_push(key="json_file_path", value=file_path)

# AWS S3 업로드 함수
def upload_to_s3(**context):
    # XCom에서 JSON 파일 경로 가져오기
    file_path = context["ti"].xcom_pull(task_ids="fetch_kopis_data", key="json_file_path")

    if not file_path:
        raise Exception("파일 경로를 찾을 수 없습니다.")

    # S3 업로드 경로 설정
    today = datetime.today().strftime("%Y%m%d")
    s3_file_path = f"{S3_FOLDER_PATH}kopis_performance_{today}.json"

    try:
        s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_file_path)
        print(f"{file_path} 파일이 S3 버킷 {S3_BUCKET_NAME}/{s3_file_path}에 업로드되었습니다.")
    except Exception as e:
        raise Exception(f"S3 업로드 실패: {e}")

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "kopis_to_s3",
    default_args=default_args,
    schedule="0 3 * * *",  # 매일 새벽 3시 실행
    catchup=False
)

# API 호출 및 JSON 저장 태스크
fetch_task = PythonOperator(
    task_id="fetch_kopis_data",
    python_callable=fetch_kopis_data,
    provide_context=True,
    dag=dag
)

# S3 업로드 태스크
upload_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag
)

# DAG 실행 순서 정의
fetch_task >> upload_task