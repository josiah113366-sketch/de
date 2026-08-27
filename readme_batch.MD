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
```
terraform -chdir=infra init
terraform -chdir=infra fmt
terraform -chdir=infra plan
terraform -chdir=infra apply
```

# 인프라 구성후 데이터 확인
- 아테나 > 본인 작업 그룹 선택 > db 선택 > 쿼리 편집기 아래 쿼리 실행
```
-- 데이터의 총개수 조회
select count(*) as cnt
from silver_logs_tbl
where year = '2026'
	and month = '08'
	and day = '26'
	and hour = '21'
;
```

# 시나리오 
- 데이터를 계속 데이터 파이프라인을 통해서 저장되어 있다 
- 파티션 기준 1시간 단위로 파티션 분리되고 있음
```
00:00 ~ 01:00 → Silver 저장
01:00 ~ 02:00 → Silver 저장
02:00 ~ 03:00 → Silver 저장
...
22:00 ~ 23:00 → Silver 저장
23:00 ~ 24:00 → Silver 저장
```