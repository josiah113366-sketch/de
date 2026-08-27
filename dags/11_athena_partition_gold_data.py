# Silver -> Gold (운영형/ 데이터는 파티션 단위로 insert 처리됨(일간 집계 데이터), 기존 데이터 삭제 x, 테이블 삭제 x)

# 1. 모듈 가져오기 
from datetime import timedelta
import pendulum 
from airflow import DAG 
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
# 하루에 여러 번 수행 시 -> 재실행 -> 기존 데이터가 대체되는 방식 필요
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
GOLD_TABLE_NAME = "gold_daily_report_tbl"

# Athena SQL 실행 결과 저장 -> [v] 직접 지정 or 작업 그룹 지정 -> 저장되는 위치가 결정
QUERY_RESULT_S3 = f"s3://{BUCKET_NAME}/athena/dags/"

# 실제 데이터 저장 위치 -> parquet 저장
GOLD_PREFIX = "gold/daily_report/"
GOLD_LOCATION = f"s3://{BUCKET_NAME}/{GOLD_PREFIX}"

# 처리 대상 날짜, 시간 등 세팅 (yyyy:MM:dd hh:mm:ss) -> 추후 실제 작동 시에는 주석 내용 반영
TARGET_DATE = "2026-08-26"  # "{{ dag_run.conf.get('target_date', ds) }}"
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
  t3_create_gold_table_with_ctas = AthenaOperator(
    task_id = "create_gold_table_with_ctas",
    # sql 
    query = f'''
      CREATE TABLE {GOLD_TABLE_NAME} 
      WITH (
        format            = 'PARQUET',
        external_location = '{GOLD_LOCATION}'
      )
      AS 
      SELECT 
        DATE('{TARGET_DATE}') AS report_date,
        domain,
        event_type,
        COALESCE( service.name,'unknown' ) AS service_name,
        COUNT(*) AS total_count,
        COUNT(response.status_code) AS response_count,
        COUNT_IF( response.status_code >= 200 AND response.status_code < 400 ) AS success_count,
        COUNT_IF( response.status_code >= 400 ) AS error_count,
        CASE
            WHEN COUNT(response.status_code) = 0 THEN 0
            ELSE ROUND( 100.0 * COUNT_IF(response.status_code >= 400) / COUNT(response.status_code), 2 )
        END
            AS error_rate_pct,
        ROUND( AVG( CAST( response.latency_ms AS DOUBLE ) ), 2 ) AS avg_latency_ms,
        MIN(response.latency_ms) AS min_latency_ms,
        APPROX_PERCENTILE( response.latency_ms, 0.95 ) AS p95_latency_ms,
        MAX(response.latency_ms) AS max_latency_ms,
        COALESCE( SUM(request.request_bytes), 0 ) AS total_request_bytes,
        COALESCE( SUM(response.response_bytes), 0 ) AS total_response_bytes
      FROM {SILVER_TABLE_NAME}
      WHERE 
        year = '{TARGET_YEAR}' 
      AND
        month = '{TARGET_MONTH}'
      AND 
        day = '{TARGET_DAY}' 
      GROUP BY
        domain, 
        event_type,
        service.name
    ''',
    # 접속 및 디비 정보
    aws_conn_id = AWS_CONN_ID, 
    database = DATABASE_NAME, 
    output_location = QUERY_RESULT_S3
    # 워크 그룹의 저장 위치가 더 우선 순위가 됨
    # workgroup = "de-ai-16-loggen-analysis"
  )

  # 5. 의존성 구성 (수행 순서 >> ) 
  t1_drop_gold_table >> t2_delete_gold_s3 >> t3_create_gold_table_with_ctas
  # t1_drop_gold_table >> t2_delete_gold_s3
  