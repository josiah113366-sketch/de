# Silver -> Gold (CTAS 방식으로 구성)
# 1. 모듈 가져오기 
from datetime import timedelta
import pendulum 
from airflow import DAG 
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

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

# CTAS가 실제로 참조하는 데이터 저장 위치 -> parquet 저장
GOLD_PREFIX = "gold/daily_report_ctas"
GOLD_LOCATION = f"s3://{BUCKET_NAME}/{GOLD_PREFIX}"

# 처리 대상 날짜, 시간 등 세팅 (yyyy:MM:dd hh:mm:ss)
TARGET_DATE = "{{ dag_run.conf.get('target_date', ds) }}"
TARGET_YEAR = "2026" # "{{ dag_run.conf.get('target_date', ds)[0:4] }}"
TARGET_MONTH = "08" # "{{ dag_run.conf.get('target_date', ds)[5:7] }}"
TARGET_DAY = "26" # "{{ dag_run.conf.get('target_date', ds)[8:10] }}"

# 3. DAG 정의 
with DAG(  
  dag_id      = "10_cats_gold_data",
  description = "Silver -> DAG + Athena -> Gold, parquet 생성",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "0 5 * * *", # 00시 05분 00초에 참고용
  start_date  = pendulum.datetime( 2026,6,29, tz=pendulum.timezone("Asia/Seoul") ),
  catchup     = False,
  tags        = ['aws', 'athena', 'ctas']
) as dag:  
  
  # 4. task 정의 (오퍼레이터 사용)
  # 4-1. 기존 CTAS Gold 테이블 삭제
  t1_drop_gold_table = AthenaOperator(
    task_id = "drop_gold_table",
    # sql 
    query = f'''
      DROP TABLE IF EXISTS {GOLD_TABLE_NAME}
    ''',
    # 접속 및 디비 정보
    aws_conn_id = AWS_CONN_ID, 
    database = DATABASE_NAME, 
    output_location = QUERY_RESULT_S3
    # 워크 그룹의 저장 위치가 더 우선 순위가 됨
    # workgroup = "de-ai-16-loggen-analysis"
  )
  # # 4-2. 기존 CTAS S3 데이터 삭제
  t2_delete_gold_s3 = S3DeleteObjectsOperator(
    task_id = "delete_gold_s3",
    bucket = BUCKET_NAME, 
    prefix = GOLD_PREFIX, 
    aws_conn_id = AWS_CONN_ID
  )
  # # 4-3. CTAS 실행 (silver sql 수행 -> 결과 -> 테이블 구성 -> 결과 데이터는 parquet 저장)
  # t3_create_gold_table_with_ctas = AthenaOperator(
  #   task_id = "create_gold_table_with_ctas",
  # )

  # 5. 의존성 구성 (수행 순서 >> ) 
  t1_drop_gold_table >> t2_delete_gold_s3
  # t1_drop_gold_table >> t2_delete_gold_s3 >> t3_create_gold_table_with_ctas