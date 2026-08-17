'''
- 활용분야
  - 소카 : 렌터카 반납 -> 사진 촬영 업로드(s3) -> 트리거(변화) -> 이미지 판독(파손///) : 분석 -> 판정
- 동작
  - 버킷내 특정 공간 감시(sensor) -> 파일 업로드 동작 -> 감지 -> DAG의 Task 작동 -> 삭제
'''
# 1. 모듈
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # 키 감시용
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator # 특정 객체 삭제

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pendulum

# 2. 환경변수

# 3. DAG
  
  # 4. task

  # 5. 의존성