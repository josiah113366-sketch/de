# Silver -> Gold (CTAS 방식으로 구성)
# 1. 모듈 가져오기 
from datetime import datetime
import pendulum 
from airflow import DAG 
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectOperator

# 2. 환경 변수
AWS_CONN_ID = "aws_default"
# 버킷
BUCKET_NAME = "de-ai-16-loggen-s3-bk-827913617635"
# 디비명
DATABASE_NAME = "de_ai_16_loggen_silver_glue_db"
# 테이블명
SILVER_TABLE_NAME = "silver_logs_tbl"

# 1회성 테이블 (24시간 유지 -> 다음번 batch 작업 시 삭제, 신규 생성 테이블), 운영 사용 테이블 겹치면 x
GOLD_TABLE_NAME = "gold_daily_report_ctas_tbl"

# Athena SQL 실행 결과 저장 -> 직접 지정 or 작업 그룹 지정 -> 저장되는 위치가 결정
QUERY_RESULT_S3 = f"s3://{BUCKET_NAME}/athena/dags/"

# CTAS가 실제로 참조하는 데이터 저장 위치 
GOLD_PREFIX = "gold/daily_report_ctas"
GOLD_LOCATION = f"s3://{BUCKET_NAME}/{GOLD_PREFIX}"

# 3. DAG 정의 

  # 4. task 정의 (오퍼레이터 사용)

  # 5. 의존성 구성 (수행 순서 >>)