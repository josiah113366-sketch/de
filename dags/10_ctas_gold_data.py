# Silver -> Gold (CTAS 방식으로 구성)
# 1. 모듈 가져오기 
from datetime import datetime
import pendulum 
from airflow import DAG 
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectOperator

# 2. 환경 변수

# 3. DAG 정의 

  # 4. task 정의 (오퍼레이터 사용)

  # 5. 의존성 구성 (수행 순서 >>)