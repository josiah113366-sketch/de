# DAG에 의해 작동 -> 데이터 기반 etl 처리 

# 1. 모듈 가져오기 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys

# 2. 인자값 추출 -> airflow에서 수행 시간 정보 전달 -> {{ ds }}등 수행 시간 정보 전달
if len(sys.argv) > 1:
  TARGET_DATE = sys.argv[1] # airflow에서 전달받은 수행일자
else:
  raise ValueError("날짜 인자가 누락되었습니다. (YYYY-MM-DD)")    

# 3. 버킷 정보 
BUCKET_NAME = "de-ai-16-loggen-s3-bk-827913617635" 
INPUT_PATH = f"s3://{BUCKET_NAME}/raw_data.json" # 나중에 필요 시 dt = {TARGET_DATE} 파티션 처리 가능함
OUTPUT_PATH = f"s3://{BUCKET_NAME}/processed/" # 나중에 필요 시 ~/processed/dt = {TARGET_DATE}/

# 4. 스파크를 통한 ETL 처리 함수 -> 브론즈 -> (정제, 처리 시간 기록) ->  실버
def clean_processing():
  # 4-1. 스파크 세션 생성
  spark = (SparkSession 
          .builder 
          .appName(f'Daily_Data_Cleaning_{TARGET_DATE}')   # airflow에서 전달된 시간 정보로 앱 이름 구성
          .getOrCreate()
          )

  # 4-2. Extract 데이터 추출, 스키마 준비, JSON 경로(브론즈 경로) -> df 

  # 4-3. Transform 정제 -> 필터
  # 4-4. Transform 파생 변수 -> 처리 시간 기록

  # 4-5. Load parquet로 저장 

  # 4-6. 스파크 세션 종료
  spark.stop() 
  pass

# 5. 엔트리 포인트 
if __name__ == "__main__":
  clean_processing()