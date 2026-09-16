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
  '''
  {
    "event_id": "88f6e21e-4eba-4104-b1c0-71ebaed11c43", "user_id": "user_252", 
    "event_type": "purchase", "product_id": 1974, 
    "price": 36892, "timestamp": "2026-09-16 11:15:48", "os": "iOS"}  
  '''
  # 4-2-1. 스키마 정의
  schema = StructType([
    StructField('event_id',   StringType(),  True),
    StructField('user_id',    StringType(),  True),
    StructField('event_type', StringType(),  True),
    StructField('product_id', IntegerType(), True),
    StructField('price',      IntegerType(), True),
    StructField('timestamp',  StringType(),  True),
    StructField('os',         StringType(),  True)
  ])
  # 4-2-2. df 구성 
  raw_df = (spark.read.schema(schema).json(INPUT_PATH)) # 지연 모드이므로 데이터 로드 x 
  # 원본 데이터 확인
  print(f"원본 데이터 개수: {raw_df.count()}") 

  # 4-3. Transform 정제 -> 필터 -> 노이즈 제거(결측, 오류값 등이 존재하는 데이터 제외)
  clean_df = ( raw_df
    .filter( F.col('user_id') )
    .filter( F.col('price') )
    .filter( F.col('timestamp') )
    .fillna( {"event_type": "unknown"} ) # 결측치를 특정 값으로 대체, 매칭으로 처리 
    .dropDuplicate( ["event_id"] ) # "event_id"가 중복되게 전달될 수 있다 (실제, 여기 코드에서는 대상 x)
  )

  # 4-4. Transform 파생 변수 -> 처리 시간 기록

  # 4-5. Load parquet로 저장 

  # 4-6. 스파크 세션 종료
  spark.stop() 
  pass

# 5. 엔트리 포인트 
if __name__ == "__main__":
  clean_processing()