'''
- airflow 내부 정보 접근, 출력 시 jinja 활용, 내부 정보 접근 시 macro 활용
'''
# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging # 레벨별 로그 출력 (에러, 경고, 정보, 디버깅, ...)
import pendulum

# 2. 전역 변수
KST = pendulum.timezone("Asia/Seoul")

# 4-1. 콜백 함수
def _print(**kwargs):
    pass

# 3. DAG 
with DAG(
    dag_id      = "03_basics_context_jinja",  
    description = "macro를 이용하여 context 접근, jinja를 통해 표현",  
    default_args = {
            "owner"           : "aic-de1-admin",     
            "retries"         : 1,                   
            "retry_delay"     : timedelta(minutes=5) 
    }, 
    # 매일 오전 9시 00분에 스케줄 작동
    schedule_interval = "0 9 * * *",  # cron 방식으로 표기 (분, 시, 일, 월, 주)
    # 수행 시작 시간 서울 시간대 타임존 조정
    start_date = pendulum.datetime(2026,6,29, tz = "KST"), 
    catchup = False, 
    tags = ['macro', 'context', 'jinja'] 
) as dag:
    # 4. 오퍼레이터를 이용하여 task를 정의
    t1 = BashOperator(
        task_id      = "jinja_used_task",
        bash_command = ""
    )
    t1 = BashOperator(
        task_id      = "jinja_macro_task",
        bash_command = ""
    )
    t1 = PythonOperator(
        task_id      = "jinja_python_task",
        python_callable = _print
    )

    # 5. 의존성