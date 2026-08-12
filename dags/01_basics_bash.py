'''
- 기본 DAG 연습
- DAG의 기본 형태가 갖춰지지 않으면 대시보드 상에 등록 X
- 필수 구성을 갖추면 잠시 후 대시보드 상에 등록됨 

- 목표 
    - bash 오퍼레이터 테스트, DAG 인식, DAG 작동 확인, DAG 기본 구성
    - 작동 확인 
        - 시각적 : 대시보드 
        - 로그
    - 본 작성 파일은 xxx-worker 컨테이너에 /opt/airflow/dags/ 하위에 동기화됨
    - 실제는 xxx-worker 컨테이너에서 가동됨 !!! 
        - 작성은 호스트 PC
'''
# 1. 필요한 모듈, 패키지 가져오기
# DAG 클래스 
from airflow import DAG
# 오퍼레이터 2.x -> 3.x에서는 패키지 경로가 변경됨
from airflow.operators.bash import BashOperator
# 스케줄 -> 시간 
from datetime import datetime, timedelta

# 2-1. default_args, 편의상 바깥에서 정의, 향후 내부에서 정의
default_args = {
    "owner"           : "aic-de1-admin",     # DAG 소유주
    "depends_on_past" : False,               # 과거 데이터(가동 시간 대비) 소급 처리 금지
    "retries"         : 1,                   # 작업 실패 시 재시도 횟수 1회 설정
    "retry_delay"     : timedelta(minutes=5) # 작업 실패 후 5분 후 재시도
    # 시나리오 
    # 작업 성공 -> 완료
    # 작업 실패 -> 5분 대기 -> 1회 재시도 -> 성공 -> 완료
    # 작업 실패 -> 5분 대기 -> 1회 재시도 -> 실패 -> 완료(실패)
    #             향후 작업이 재개되도(다음 스케줄에 의해) -> 누락된 과거 데이터 소급 X (백필 X)
}

# 2. DAG 정의 -> DAG 세션이 오픈된다 의미
with DAG(
    dag_id      = "01_basics_bash",  # DAG간 구분하는 용도 
    description = "DE 업무 중 배치 파이프라인 구성 중 오케스트레이션 담당 airflow의 DAG 작성 기본형",  #DAG 설명
    default_args = default_args, 
    schedule_interval = "@daily", # 하루에 한 번 00시 00분 00초, 문자열, cron 표현 (* * * *)
    start_date = datetime(2026,6,29), # 현재 기준 갭이 발생 -> 소급 처리 X (위 설정 처리 X)
    catchup = False, # 과거에 대한 소급 처리 실행 방지
    # 기본 설정에서 소급 X, 오늘 기준으로 8월 13일 00시 00분 00초에 작동
    tags = ['bash', 'basic'] # DAG 검색(특정)을 위해서 자유롭게 세팅
) as dag:

    # 3. Operator 정의 

    # 4. 의존성 정의, 구동 순서 정의

    pass