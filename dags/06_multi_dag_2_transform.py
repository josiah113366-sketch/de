# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
# 핵심 클레스
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
def _transform(**kwargs):
    # 1. 다른 DAG에서 전달한 내용을 가져온다 
    dag_run             = kwargs["dag_run"]
    json_file_path      = dag_run.conf.get('json_path')
    logging.info(f'전달한 데이터 파일 경로 {json_file_path}')

    # 2. transform -> 데이터 clean, 전처리(단위 변경, 파생 변수, ...)
    #    섭씨 온도를 화씨 온도로 계산 -> 파생 변수 추가 -> pandas의 DataFrame 활용
    #    섭씨 온도 100도 이하(<=)만 센서가 정상, 그 이상은 이상 탐지의 대상으로 간주 -> 이상치 제거(컨셉)
    # 2-1. json file -> load -> DataFrame
    df = pd.read_json(json_file_path)
    # 2-2. 이상치 제거(컨셉) -> clean 작업의 범주, 100도 이하만(조건->불린) 추출(인덱싱): 불린 인덱싱
    target_df= df[df['temperature'] <= 100].copy()
    # 2-3. 파생 변수 생성 -> 섭씨 -> 화씨 
    # °F = (°C × 9/5) + 32
    target_df["temperature_f"] = (target_df['temperature'] * 9/5) + 32

    logging.info(f'가공된 데이터 (rows, cols) {target_df.shape}')
    # 3. 전처리된 내용 [v]csv|parquet|... 저장 
    csv_file_path = f"{DATA_PATH}/preprocessing_data_{kwargs['ds_nodash']}.csv"
    target_df.to_csv(csv_file_path, index=False)
    logging.info(f'가공된 데이터 저장 {csv_file_path}')

    # 4. 반환, XCOM 전달, 게시
    return csv_file_path

# 3. DAG
with DAG(
    dag_id      = "06_multi_dag_2_transform",   
    description = "transform 전용 DAG",  
    default_args = {
            "owner"           : "aic-de1-admin",     
            "retries"         : 1,                   
            "retry_delay"     : timedelta(minutes=1) 
    }, 
    schedule_interval = "@daily",  
    start_date = pendulum.datetime(2026,6,29, tz = KST), 
    catchup = False, 
    tags = ['etl', 'transform'] 
) as dag:
    # 4. 오퍼레이터
    task_transform = PythonOperator(
        task_id = "transform",
        python_callable = _transform
    )
    # 신규 추가 부분
    task_trigger_load_dag_run = TriggerDagRunOperator(
      task_id = "trigger_load", 
      # 트리거의 대상, 다음에 구동시킨 DAG id 
      trigger_dag_id = "06_multi_dag_3_load", 
      # 구동시킬 때 전달할 데이터 -> xcom을 통해서 획득 + jinja 활용
      conf = {
        # 항목은 커스텀 구성
        # 쌍따옴표와 중괄호 사이 비워두면 안 됨 
        "csv_path" : "{{ task_instance.xcom_pull(task_ids='transform') }}"
      }, 
      # dag 최초 수행 시간 세팅 -> 첫번째 DAG과 동일하게 두번째 DAG로 해당 시간으로 최초 수행 시간으로 간주할지 
      reset_dag_run=True,
      # 기타 설정
      # 다음 DAG가 수행되는 것을 보고(대기) 종료할 것인가?(동기식) 명령 전달 후 바로 종료?(비동기)
      wait_for_completion= False # 명령 전달 후 바로 종료
    )

    # 5. 의존성, 작동 순서 정의
    task_transform >> task_trigger_load_dag_run