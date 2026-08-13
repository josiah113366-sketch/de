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

# 3. DAG 

    # 4. 오퍼레이터

    # 5. 의존성