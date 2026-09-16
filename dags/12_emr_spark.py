# 1. 모듈 가져오기
from datetime import timedelta
import pendulum
from airflow import DAG
# 오퍼레이터 
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# 2. 환경 변수
BUCKET_NAME = 'de-ai-25-loggen-s3-bk-827913617635'
SPARK_SCRIPT_PATH = f"s3://{BUCKET_NAME}/spark/scripts/spark_etl.py" 
EMR_LOG_URI = f"s3://{BUCKET_NAME}/spark/emr_logs/" 

# 3. 인프라 설정 dict 형태 : json 구조로 구성 
# 장점 : 스케줄, 프로그램과 같이 진행 -> 관리 용이, 필요할 때 사용할 때 유용 
JOB_FLOW_OVERRIDES = {
  # 클러스터 구분용, 리소스 id는 내부적으로 반환됨 
  "Name": "Airflow-Automated-EMR-Cluster-de16",
  # EMR 소프트웨어의 버전 지정 -> EC2 + hadoop + spark + emr 환경 구성
  "ReleaseLabel": "emr-6.10.0",
  # Hadoop, spark 설치하도록 지정
  # Hadoop 환경은 스파크 구동에 기반 역할 담당
  "Applications": [
      {"Name": "Hadoop"},
      {"Name": "Spark"},
  ],
  # EC2 기반 EMR 구성
  # 인스턴스 생성
  "Instances": {
      # 테라폼 -> VPC 구성 -> 동적 세팅 
      # 약식 구성 : 기본 VPC의 a존의 서브넷 고정 
      "Ec2SubnetId": "subnet-0928223142a64ef05",
      "InstanceGroups": [
          # 테스트 하면서 데이터량에 맞춰 수치/사양 조정해야 함 -> 개발자가 직접 구성, 운영은 마스터가 알아서 함 
          # 마스터, 워커 노드가 운용될 ec2 인스턴스(머신) 구성
          # EMR 클러스터 관리용(스파크 작업 관리, 리소스 관리, 워커 노드 관리) 노드 
          # 데이터 증가 -> 현재 구성상 관리 힘듦 -> 용량 증가하라 등 처리 
          {
              # 마스터
              "Name": "Master node",
              # 스팟 : 비용 저렴, EC2 용량 대비 할인 가격을 사용. 단, 시스템에 의해 중단 가능성 존재함 
              # Master는 안정성이 중요해서 
              "Market": "SPOT",
              # "Market": "ON_DEMAND", # <- 이 구성이 더 나음  
              "InstanceRole": "MASTER",
              "InstanceType": "m5.xlarge",
              "InstanceCount": 1,
          },
          {
              # 워커 
              "Name": "Core nodes",
              "Market": "SPOT",
              "InstanceRole": "CORE",
              "InstanceType": "m5.xlarge",
              # 인스턴스 2개 지정 -> 작업량에 따라 관리 
              "InstanceCount": 2,
          },
      ],
      # 스파크 작업이 끝났다고 스스로 종료 X -> EmrTerminateJobFlowOperator에 의해 명시적 종료되게 TRUE 적용
      "KeepJobFlowAliveWhenNoSteps": True,
      # 종료 보호 장치 해제, True이면 EmrTerminateJobFlowOperator와 충돌날 수 있음 
      "TerminationProtected": False,
    },
    # 최소 권한 부여했다 
    "JobFlowRole": "EMR_EC2_DefaultRole", # EC2 인스턴스가 사용하는 IAM ROlE 
    "ServiceRole": "EMR_DefaultRole", # EMR 서비스에 대한 기본 IAM ROLE  
    # EMR 로그 저장 위치 지정 
    "LogUri": EMR_LOG_URI,
    # 클러스터의 가시성 설정 
    "VisibleToAllUsers": True,
} 
SPARK_SUBMITS = [
  {
    "Name": "Daily Data Cleaning Job",
    "ActionOnFailure": "CONTINUE",      # 작업이 실패 나더라도 다음 스텝 진행을 위한 구성
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",             # 스파크 구동을 위한 명령어 
            "--deploy-mode", "cluster", # 스파크 구동 환경 -> 클러스터 지정
            SPARK_SCRIPT_PATH,
            "2026-09-16"                # 임시 편성, ds 등 값을 획득하여 처리 
        ],
    },
  }
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