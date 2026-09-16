# 1. 모듈 가져오기
from datetime import timedelta
import pendulum
from airflow import DAG
# 오퍼레이터 
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# 2. 환경 변수

# 3. 인프라 설정 dict
JOB_FLOW_OVERRIDES = {} 
SPARK_SUBMITS = [
   {}
]

# 4. 콜백 함수 
def _dummy_task_cb():
    print("클러스터 생성 완료")

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
  create_cluster_task    = EmrCreateJobFlowOperator( # EMR 클러스터 생성 (스파크 구동하기 위한 인프라 구성)
    task_id = "create_cluster", 
    # 인프라 구성 dict로 표현 == 테라폼의 resource "aws_emr_cluster" {}
    job_flow_overrides = JOB_FLOW_OVERRIDES,
    # aws 연결 정보 
    aws_conn_id = "aws_default"
    # 인프라 구성 후 클러스터를 참조할 수 있는 리소스 id를 자동 반환
  )
  dummy_task             = PythonOperator( # 더미 작업, 인프라 구성 완료됨을 확인, 생략 가능함 
    task_id = "dummy",
    python_callable = _dummy_task_cb 
  )
  run_spark_task         = EmrAddStepsOperator( # 스파크 코드 작동 ETL 처리 
    task_id = "run_spark", 
    # 스파크가 작동할 환경 -> 클러스터의 id를 세팅 
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    # 스파크 지정 -> 도커 -> spark_submit ... 실행
    steps = SPARK_SUBMITS, 
    aws_conn_id = "aws_default"
  )
  watch_spark_task       = EmrStepSensor( # 센서를 통해서 스파크 작업 완료 여부 확인 
    task_id = "watch_spark",
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    # 스파크 작업 완료 여부 체크 
    step_id = "{{ task_instance.xcom_pull(task_ids='run_spark', key='return_value')[0] }}",
    aws_conn_id = "aws_default"
  )
  terminate_cluster_task = EmrTerminateJobFlowOperator( # EMR 클러스터 해제
    task_id = "terminate_cluster",
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id = "aws_default"
    trigger_rule = "all_done" # 위의 task 실패하더라도, 반드시 emr 삭제한다 

  )

  # 7. 의존성 
  create_cluster_task >> dummy_task >> run_spark_task >> watch_spark_task >> terminate_cluster_task
  pass