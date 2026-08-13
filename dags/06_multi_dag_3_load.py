# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Load 처리 시 데이터 밀어 넣기 시 활용
from airflow.providers.mysql.hooks.mysql import MySqlHook
# 핵심 클레스
from datetime import datetime, timedelta
import logging
import pendulum
import json
import random
import pandas as pd
import os

# 2. 전역 변수
KST = pendulum.timezone("Asia/Seoul")
# extract한 데이터를 임시로 저장하는 위치
DATA_PATH = "/opt/airflow/dags/data"
os.makedirs(DATA_PATH, exist_ok=True)

# 콜백 함수 
# _extract는 파일 나눴을 때 그대로 사용함
def _load(**kwargs):
    # 1. csv 경로 획득 
    dag_run             = kwargs["dag_run"]
    csv_file_path      = dag_run.conf.get('csv_path')
    logging.info(f'전달한 데이터 파일 경로 {csv_file_path}')

    # 2. csv -> df 로드 
    # read csv 
    df = pd.read_csv(csv_file_path)

    # 3. mysql 연결 -> 데이터 삽입
    hooks = MySqlHook(mysql_conn_id="mysql_default")
    # conn = hooks.get_conn()
    try:
        with hooks.get_conn() as conn:
            logging.info(f'커넥션 획득 완료')
            # 1. 커서 획득
            with conn.cursor() as cursor:
                # 2. insert 구문 작성
                sql = '''
                    insert into sensor_readings
                    (sensor_id, timestamp, temperature_c, temperature_f)
                    values 
                    (%s, %s, %s, %s)
                '''
                # 3. param 구성 (여러 데이터 세팅)
                params = [
                    (data["sensor_id"], data["timestamp"], data["temperature"], data["temperature_f"])
                    for _, data in df.iterrows() # 데이터가 없을 때까지 반복, 1세트씩 (인덱스, 데이터) 반환
                ]
                # 4. 쿼리 실행
                cursor.executemany(sql, params ) # n개 데이터 한 번에 넣기
                # 5. 커밋
                conn.commit
                pass
    except Exception as e:
        logging.error(f'sql 에러 {e}')
    else:
        logging.info(f'데이터베이스 처리 정상')
    finally:
        logging.info(f'데이터베이스 작업 완료')
        pass
    pass

# 3. DAG
with DAG(
    dag_id      = "06_multi_dag_3_load",   
    description = "load 전용 DAG",  
    default_args = {
            "owner"           : "aic-de1-admin",     
            "retries"         : 1,                   
            "retry_delay"     : timedelta(minutes=1) 
    }, 
    schedule_interval = "@daily",  
    start_date = pendulum.datetime(2026,6,29, tz = KST), 
    catchup = False, 
    tags = ['etl', 'load'] 
) as dag:
    # task 정의 
    task_create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        # 접속 정보 설정 -> 대시보드 > admin > connection 구성한 값 설정 -> id 값
        conn_id = "mysql_default", 
        # 테이블이 없을 때만 구성
        sql             = '''
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sensor_id VARCHAR(50),
            timestamp DATETIME,
            temperature_c FLOAT,
            temperature_f FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
    ) 

    task_load = PythonOperator(
        task_id = "load",
        python_callable = _load
    )
    # 5. 의존성, 작동 순서 정의
    task_create_table >> task_load