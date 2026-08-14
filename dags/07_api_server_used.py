'''
- 고객 정보가 담긴 데이터베이스가 있음
    CREATE TABLE IF NOT EXISTS customers (
        user_id VARCHAR(50) PRIMARY KEY,
        income INT DEFAULT NULL,
        loan_amt INT DEFAULT NULL,
        credit_score INT DEFAULT NULL,
        grade VARCHAR(10) DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
- 매일 고객이 가입, 업무 등등 디비 내용이 갱신 (설정)
- 다음날 00시 01분 00초에 고객 디비 가져와서(extract) -> 신용 평가(api 서버 요청) -> 평가 결과 획득 -> 고객 정보 업데이트
    - 갱신 주기는 변경될 수 있다 (회사별 상이)
- 배치 데이터 프로세스 작업 - airflow
    - TASK 정리
    - t1 : mysql 사용, 테이블이 없으면 생성, 고객 데이터 더미 입력(매번 수행-해쉬(UUID) 적용) 
        - 원래 배치 작업에서는 필요 없는 작업임
    - t2 : 고객 데이터 획득 (DB -> DAG의 TI) -> XCOM 게시 df or dict
    - t3 : XCOM 데이터 획득 -> API 호출 -> 서버 고객 데이터 전송 -> 신용 평가 진행 -> 응답 -> XCOM 게시 
    - t4 : XCOM 데이터 획득 -> 신용 평가 결과 -> 고객 DB 업데이트 
'''