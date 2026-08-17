'''
- airflow aws를 엑세스 -> 오퍼레이터등 도구 제공 -> 패키지 설치
- docker-compose.yaml
  - apache-airflow-providers-amazon 추가
  - _PIP_ADDITIONAL_REQUIREMENTS: ...viders-mysql apache-airflow-providers-amazon

- docker compose down
- docker compose up -d

- 로컬 설치 : pip install apache-airflow-providers-amazon
- 원격 PC에서 AWS S3의 특정 버킷(보인 소유의)에 간단하게 데이터 업로드 테스트 DAG
'''
# 1. 모듈 가져오기
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import pendulum

# 2. 환경변수(전역변수) -> .env, airflow 환경변수
BUCKET_NAME      = "de-ai-16-infra-s3-bk-827913617635"
UPLOAD_FILE_NAME = "costs.csv" # 각자 다를수 있음
LOCAL_PATH       = f"/opt/airflow/dags/data/{UPLOAD_FILE_NAME}" # 컨테이너에서 업로드 경로

# 3. DAG 정의
  # 4. task 
  # 5. 의존성

