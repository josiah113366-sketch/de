# DAG에 의해 작동 -> 데이터 기반 etl 처리

# 1. 모듈 가져오기
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys

# 2. 인자값 추출 -> airflow에서 수행시간 정보 전달 => {{ ds }}등 수행시간 정보 전달
if len(sys.argv) > 1:
    TARGET_DATE = sys.argv[1]
else:
    raise ValueError("날짜 인자가 누락되었습니다. (YYYY-MM-DD)")

# 3. 버킷 정보
BUCKET_NAME = 'de-ai-25-loggen-s3-bk-827913617635'
INPUT_PATH  = f"s3://{BUCKET_NAME}/raw_data.json" # 나중에 필요시 dt={TARGET_DATE} 식으로 파티션 처리 가능함
OUTPUT_PATH = f"s3://{BUCKET_NAME}/processed/"    # 나중에 필요시 ~/processed/dt={TARGET_DATE}/

# 4. 스파크를 통한 ETL 처리 함수 -> 브론즈 =>(정제, 처리시간기록)=> 실버
def clean_processing():
    # 4-1. 스파크 세션 생성
    spark = (SparkSession
             .builder
             .appName(f'Daily_Data_Cleaning_{TARGET_DATE}') # airflow에서 전달된 시간정보로 앱 이름 구성
            .getOrCreate()
    )

    # 4-2. Extract 데이터 추출, 스키마 준비, JSON경로(브론즈경로) => df
    '''
    {
     "event_id": "7a565719-41e9-4b36-9ce7-c4bfde531f8c", "user_id": "user_442", 
     "event_type": "purchase", "product_id": 1287, 
     "price": 35215, "timestamp": "2026-09-16 09:15:31", "os": "Windows"
    }
    '''
    # 4-2-1. 스키마 정의
    schema = StructType([
        StructField('event_id',     StringType(),  True),
        StructField('user_id',      StringType(),  True),
        StructField('event_type',   StringType(),  True),
        StructField('product_id',   IntegerType(), True),
        StructField('price',        IntegerType(), True),
        StructField('timestamp',    StringType(),  True),
        StructField('os',           StringType(),  True)
    ])
    # 4-2-2. df 구성
    raw_df = spark.read.schema(schema).json(INPUT_PATH) # 지연모드이므로 데이터 로드 x
    # 원본 데이터 확인
    print(f'원본 데이터 개수 {raw_df.count()}')

    # 4-3. Transform 정제 -> 필터 => 노이즈 제거(결측, 오류값등이 존재하는 데이터 제외)
    clean_df = ( raw_df
      .filter( F.col('user_id').isNotNull()  ) # 결측치 제거 => 아이디가 있는 데이터만 포함
      .filter( F.col('price') > 0            ) # 0보다 크면 모두 포함
      
      
      .withColumn('event_time', F.to_timestamp( F.col('timestamp'), "yyyy-MM-dd HH:mm:ss")) #'event_time' 파생 컬럼 생성
      .filter( F.col('event_time').isNotNull() ) # 타입변환->노이즈 결측됨->결측제외->데이터 획득

      .fillna( {"event_type":"unknown" } ) # 결측치를 특정값 대체, 매칭으로 처리
      .dropDuplicates(["event_id"])        # "event_id"가 중복되게 전달될수 있다(실제, 여기 코드에서는 대상 x)
    )

    # 4-4. Transform 파생변수 -> 처리시간기록
    final_df = ( clean_df.withColumn( 'processed_at', F.current_timestamp()) # 처리시간 기록
    # timestamp 컬럼을 제외하려고 한다면 select() 활용 
    print(f'데이터 전처러 후 개수 {final_df.count()}') 

    # 4-5. Load parquet로 저장
    final_df.write.mode("overwrite").parquet(OUTPUT_PATH) # s3 저장 

    # 4-6. 스파크 세션 종료
    spark.stop()
    pass



# 5. 엔트리 포인트
if __name__=='__main__':
    clean_processing()