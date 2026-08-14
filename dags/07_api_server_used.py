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
'''