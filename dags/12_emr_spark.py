# 1. 모듈 가져오기
from datetime import timedelta
import pendulum
from airflow import DAG

# 2. 환경 변수

# 3. 인프라 설정 dict

# 4. 콜백 함수 

# 5. dag 정의 
with DAG(  
  dag_id      = "12_EMR_SPARK",
  description = "대용량 데이터를 분산 환경에서 ETL하기 위해 스파크 사용",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "daily", 
  start_date  = pendulum.datetime( 2026,6,29, tz=pendulum.timezone("Asia/Seoul") ),
  catchup     = False,
  tags        = ['aws', 'spark', 'emr']
) as dag: 

  # 6. task 구성 

  # 7. 의존성 
  pass