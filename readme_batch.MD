# 목표
- Silver layer -> Gold Layer로 데이터 파이프라인 연결 
- 실시간 x, 배치 작업 o 
  - 매일 00시 5분 00초에 airflow에 등록된 DAG에 의해서 집계 처리... Gold에 필요로 하는 형태로 데이터 처리
    - pandas/polars/spark를 활용 처리 <- 4 챕터(AI 활용 시 사용)
    - [v] athena/opensearch/redshift 등 aws 서비스 활용 처리 <- 활용
      - [v] SQL 사용 -> athena (서버리스)
      - [v] 인덱스 검색 -> opensearch
      - redshift 엔터프라이즈 대용량 데이터 처리용, 열 검색 ... 

# 인프라 구성
- s3.tf : 보정 (기존 s3 버킷 사용하도록 이름, 태그 조정, 삭제 여부 조정)
- 