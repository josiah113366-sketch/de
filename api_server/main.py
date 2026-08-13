'''
- 평가를 해야 하는 고객 데이터 구조(요청/응답)
    - [ {}, {}, ...]
'''
# 1. 모듈 가져오기 
from fastapi import FastAPI     # 앱
from pydantic import BaseModel  # 요청/응답 클래스 구성 시 슈퍼 클래스 역할
from typing import List         # 요청/응답 데이터 구성 시 구조 정의시 사용 
import random                   # 신용 평가 시 활용

# 2. FastAPI 객체 생성

# 3. 요청/응답 구조 정의 -> class

# 4. 라우팅 : url, 처리 함수 매핑 정의