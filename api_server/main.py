'''
- 평가를 해야 하는 고객 데이터 구조(요청/응답) (가장 스탠다드 형태)
    - [ {}, {}, ...] 
'''
# 1. 모듈 가져오기 
from fastapi import FastAPI     # 앱
from pydantic import BaseModel  # 요청/응답 클래스 구성 시 슈퍼 클래스 역할
from typing import List         # 요청/응답 데이터 구성 시 구조 정의시 사용 
import random                   # 신용 평가 시 활용

# 2. FastAPI 객체 생성
app = FastAPI()

# 3. 요청/응답 구조 정의 -> class
class ReqData(BaseModel):
  # 컬럼 나열 
  user_id:str  # 사용자 아이디
  income: int  # 소득 
  loan_amt:int # 현재 총 대출액

class ResData(BaseModel):
  # 컬럼 나열 
  user_id:str       # 사용자 아이디
  credit_score: int # 0점 ~ 1000점
  grade:str         # S급, A급, B급, C급, ... 

# 4. 라우팅 : url, 처리 함수 매핑 정의