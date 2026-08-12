'''
- PythonOperator 사용 패턴
- task간 통신 -> XCom 사용 (airflow 내부 컨텍스트 공간을 접근(액세스), 게시판) -> task 상호 대화(통신)
- 공간의 한계 -> 공유 데이터는 raw 데이터가 아닌 raw 데이터에 접근 가능한 정보/작은 규모 raw 가능
'''
# 1. 모듈 가져오기

# 2. DAG 정의 

# 3. Operator 정의

# 4. 의존성 정의