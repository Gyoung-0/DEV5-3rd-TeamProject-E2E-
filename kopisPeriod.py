from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import csv
import boto3
import xml.etree.ElementTree as ET
import os
import calendar

# Airflow 환경 변수에서 가져오기
KOPIS_API_KEY = Variable.get("KOPIS_API_KEY")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = "dev-kopis"
S3_FOLDER_PATH = "kopis_period/"

# KOPIS API URL
BASE_URL = "http://kopis.or.kr/openApi/restful/prfstsTotal"

# AWS S3 클라이언트 설정
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


def fetch_and_upload_kopis_data(**context):
    """
    2023년 1월부터 현재까지 반복 실행하여 데이터를 가져오고 CSV로 변환하여 S3에 업로드.
    """
    # 현재 날짜 가져오기
    now = datetime.today()

    # 2018년 1월부터 현재 연월까지 반복 실행
    for year in range(2018, now.year + 1):
        for month in range(1, 13):
            # 현재 연월보다 미래면 종료
            if year == now.year and month > now.month:
                break
            
            # 해당 월의 시작일과 마지막일 구하기
            start_date = f"{year}{month:02d}01"
            last_day = calendar.monthrange(year, month)[1]
            end_date = f"{year}{month:02d}{last_day}"

            # API 요청
            params = {
                "service": KOPIS_API_KEY,
                "ststype": "day",
                "stdate": start_date,
                "eddate": end_date
            }
            response = requests.get(BASE_URL, params=params)

            # API 응답 확인
            if response.status_code != 200:
                print(f"⚠️ {year}-{month:02d} 데이터 요청 실패! 상태 코드: {response.status_code}")
                continue

            # XML 응답을 CSV 데이터로 변환
            try:
                root = ET.fromstring(response.text)
                records = []

                for prfst in root.findall("prfst"):
                    record = {
                        "prfcnt": prfst.find("prfcnt").text,
                        "ntssnmrs": prfst.find("ntssnmrs").text,
                        "cancelnmrs": prfst.find("cancelnmrs").text,
                        "amount": prfst.find("amount").text,
                        "nmrs": prfst.find("nmrs").text,
                        "prfdtcnt": prfst.find("prfdtcnt").text,
                        "prfdt": prfst.find("prfdt").text,
                        "prfprocnt": prfst.find("prfprocnt").text
                    }
                    records.append(record)

                # CSV 파일 저장 경로
                csv_filename = f"kopis_period_{year}{month:02d}.csv"
                csv_path = os.path.join("/tmp", csv_filename)

                # CSV 파일 생성
                with open(csv_path, mode="w", encoding="utf-8", newline="") as file:
                    writer = csv.DictWriter(file, fieldnames=records[0].keys())
                    writer.writeheader()
                    writer.writerows(records)

                print(f"✅ CSV 저장 완료: {csv_path}")

                # S3 업로드 경로 설정
                s3_file_path = f"{S3_FOLDER_PATH}{csv_filename}"

                # S3 업로드
                s3_client.upload_file(csv_path, S3_BUCKET_NAME, s3_file_path)
                print(f"✅ S3 업로드 완료: s3://{S3_BUCKET_NAME}/{s3_file_path}")

            except Exception as e:
                print(f"❌ XML 파싱 오류 {year}-{month:02d}: {e}")
                continue


# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "kopis_historical_to_s3",
    default_args=default_args,
    schedule="0 3 1 * *",  # ✅ 매월 1일 실행하여 전월 데이터 적재
    catchup=True  # ✅ 과거 데이터까지 적재
)

# KOPIS 데이터 수집 및 S3 업로드 태스크
fetch_and_upload_task = PythonOperator(
    task_id="fetch_and_upload_kopis_data",
    python_callable=fetch_and_upload_kopis_data,
    provide_context=True,
    dag=dag
)

fetch_and_upload_task