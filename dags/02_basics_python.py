'''
- PythonOperator 사용 패턴
- task간 통신 -> XCom 사용 (airflow 내부 컨텍스트 공간을 접근(액세스), 게시판) -> task 상호 대화(통신)
- 공간의 한계 -> 공유 데이터는 raw 데이터가 아닌 raw 데이터에 접근 가능한 정보/작은 규모 raw 가능
'''
# 1. 모듈 가져오기
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging # 레벨별 로그 출력 (에러, 경고, 정보, 디버깅, ...)

# 2. DAG 정의 
with DAG(
    dag_id      = "02_basics_bash",  
    description = "파이썬 task, XCOM 사용",  
    default_args = {
            "owner"           : "aic-de1-admin",     
            "retries"         : 1,                   
            "retry_delay"     : timedelta(minutes=5) 
    }, 
    schedule_interval = "@once",  # 수동으로 한 번 수행, 주기성 X
    start_date = datetime(2026,6,29), 
    catchup = False, 
    tags = ['python', 'xcom'] 
) as dag:
    # `ET`L
    # 3. Operator 정의
    extract_task = PythonOperator()
    transform_task = PythonOperator()

    # 4. 의존성 정의
    extract_task >> transform_task