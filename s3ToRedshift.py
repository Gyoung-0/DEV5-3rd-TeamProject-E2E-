from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import logging

# Airflow 환경 변수에서 Snowflake & S3 정보 가져오기
SNOWFLAKE_CONN_ID = "snowflake_conn"
STAGE_NAME = "S3_STAGE"
TABLE_NAME = "gyoung.kopis_performance_raw"  # JSON 적재용 테이블

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "s3_to_snowflake_kopis",
    default_args=default_args,
    schedule=None,  # 수동 실행, 필요하면 일정 추가 가능
    catchup=False
)

# 📌 Snowflake COPY 실행 함수
def load_s3_to_snowflake():
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    copy_sql = f"""
        COPY INTO {TABLE_NAME}
        FROM @{STAGE_NAME}
        FILE_FORMAT = (TYPE = JSON);
    """

    logging.info(f"Executing COPY command: {copy_sql}")
    
    try:
        snowflake_hook.run(copy_sql)
        logging.info(f"✅ S3에서 Snowflake로 데이터 적재 완료.")
    except Exception as e:
        logging.error(f"❌ Snowflake 데이터 적재 실패: {e}")
        raise

# S3 → Snowflake 적재 태스크
load_to_snowflake = PythonOperator(
    task_id="load_s3_to_snowflake",
    python_callable=load_s3_to_snowflake,
    dag=dag
)

load_to_snowflake