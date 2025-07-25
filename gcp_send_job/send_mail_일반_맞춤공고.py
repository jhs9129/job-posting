import boto3
from botocore.exceptions import ClientError
import mysql.connector
import json
import os
import logging
import re
# import mariadb
import asyncio
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine

import time
import logging
import copy
import pandas as pd
import random
import gspread
import urllib.parse
from google.oauth2.service_account import Credentials

# sendgrid 메일
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from _call_ import * 

load_dotenv()

# selenium 설정
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")


# html 불러오기
html_header_tr = read_html_file('./HTML_CODE/jobdori_header.html')
html_site_tr = read_html_file('./HTML_CODE/jobdori_site_tr.html')
html_job_tr = read_html_file('./HTML_CODE/jobdori_job_tr.html')

# api_gateway 
api_gateway_url = "http://35.216.93.183/log"

site_name_kr = {'saramin': '사람인','wanted': '원티드','jumpit': '점핏','incruit': '인크루트'}

# Data Analyst
# Data Engineer
# Data Scientist and AI
# FULLSTACK
# SERVICE
# CONTENT

# 포함, 제외 키워드 정하기
def create_job_conditions(course):
    # course_upper = course.upper()
    if "Data Analyst" in course:
        return JOB_KEYWORDS["Data Analyst"], STOPWORDS["Data Analyst"]
    elif "Data Engineer" in course:
        return JOB_KEYWORDS["Data Engineer"], STOPWORDS["Data Engineer"]
    elif "Data Scientist and AI" in course:
        return JOB_KEYWORDS["Data Scientist and AI"], STOPWORDS["Data Scientist and AI"]
    elif "FULLSTACK" in course:
        return JOB_KEYWORDS["FULLSTACK"], STOPWORDS["FULLSTACK"]
    elif "SERVICE" in course:
        return JOB_KEYWORDS["SERVICE"], STOPWORDS["SERVICE"]
    elif "CONTENT" in course:
        return JOB_KEYWORDS["CONTENT"], STOPWORDS["CONTENT"]
    else:
        return []

# course : 과정명(ex.BIGDATA) / job_sites : ['saramin', 'wanted', 'jumpit', 'incruit']
def keyword_query(course, job_sites):
    db_connection = connect_to_job()
    query_file = "./query_base.sql"
    
    # 포함키워드, 제외키워드 리스트
    include, exclude = create_job_conditions(course)

    # 빈 딕셔너리 만들기 (sql 쿼리문에 들어갈 포함키워드, 제외키워드 쿼리문으로 만들기위해서)
    include_dict = {} # 공고명(job_title) 적용
    exclude_dict1 = {} # 공고명(job_title) 적용
    exclude_dict2 = {} # 기업명(company_name) 적용

    # 채용공고 사이트 별로 쿼리문 작성
    for site in job_sites:
        include_dict[f"include_keyword_{site}"] = " OR ".join([f"{site}_data.job_title LIKE '%{word}%'" for word in include])
        exclude_dict1[f"exclude_keyword_{site}"] = " AND ".join([f"{site}_data.job_title NOT LIKE '%{word}%'" for word in exclude])
        exclude_dict2[f"exclude_keyword_{site}"] = " AND ".join([f"{site}_data.company_name NOT LIKE '%{word}%'" for word in exclude])

    # 딕셔너리 컴프리헨션으로 쿼리문 만들기~
    query_dict = {
            site: f"({include_dict[f'include_keyword_{site}']}) AND ({exclude_dict1[f'exclude_keyword_{site}']}) AND ({exclude_dict2[f'exclude_keyword_{site}']})"
            for site in job_sites
        }

    with open(query_file, "r", encoding="utf-8") as file:
        query_content = file.read()

    for site in job_sites:
        query_content = query_content.replace(f"{{{site}}}", query_dict[site])
    
    cursor = db_connection.cursor()
    cursor.execute(query_content)
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    df['course'] = course

    print(f'[{course}--------------총 공고 수{len(df)}]')
    
    ##################### 이전에 보낸 공고 삭제 ######################
    select_sql = """SELECT recruit_url from send_job;"""

    cursor.execute(select_sql)
    urls = cursor.fetchall()
    urls_list = [row[0] for row in urls]
    cursor.close()
    db_connection.close()
    
    # 빠른 테스트를 위함.
    # df = df.groupby('source_table').head(5).reset_index(drop=True)
    df = df[~df['recruit_url'].isin(urls_list)]
    ################################################################


    #################### 최신순으로 전처리 #########################
    df_date = df[df['deadline'].str.contains(r'^\d{4}-\d{2}-\d{2}$', na=False)].copy()
    df_nondate = df[~df['deadline'].str.contains(r'^\d{4}-\d{2}-\d{2}$', na=False)].copy()

    df_date['deadline'] = pd.to_datetime(df_date['deadline']).dt.strftime('%Y-%m-%d')

    df_date_sorted = df_date.sort_values(by='deadline', ascending=False)

    df = pd.concat([df_date_sorted, df_nondate], ignore_index=True)
    ###############################################################
    return  df


# 마감공고 및 이전 보낸 공고 파악
def job_check(df):
    stop_words = ['마감된 공고', '마감된 포지션', '지원마감', '접수마감']
    bad = []
    good = []
    seen = set()
    driver = webdriver.Chrome(options)
    driver.set_window_size(1000, 1350)

    today = datetime.today()

    for idx, row in df.iterrows():
        time.sleep(random.uniform(1, 4))
        url = row['recruit_url']
        source = row['source_table']
        deadline_str = str(row['deadline']).strip()

        # 제공 할 공고에서 과정 별 카운트
        source_counts = {
            'saramin': 0,
            # 'wanted': 0,
            # 'incruit': 0,
            'jumpit': 0
        }
        for g in good:
            s = g['source_table']
            if s in source_counts:
                source_counts[s] += 1

        # 만약 현재 사이트의 공고가 이미 10개면 건너뜀
        if source_counts.get(source, 0) >= 3:
            print(f"[PASS]-- [{source}] 10개 초과, 건너뜀: {row['company_name']} - {row['job_title']}")
            continue

        # 날짜 형식 확인 후 7일 이하 남은 공고는 건너뜀
        try:
            deadline_date = datetime.strptime(deadline_str, '%Y-%m-%d')
            days_left = (deadline_date - today + timedelta(days=1)).days
            if days_left < 1:
                print(f"[SKIP] {row['company_name']} - {row['job_title']} ({days_left}일 남음)")
                continue
        except:
            deadline_date = None

        # 공고 확인
        driver.get(url)
        try:
            if 'jumpit' in url:
                text = driver.find_element(By.CLASS_NAME, 'sc-190eb830-1.gaBEGj').text
            # elif 'wanted' in url:
            #     text = driver.find_element(By.CLASS_NAME, 'WantedApplyBtn_container__lBx_L').text
            # elif 'incruit' in url:
            #     text = driver.find_element(By.CLASS_NAME, 'btn_bgrp.btn_jobapp.off').text
            elif 'saramin' in url:
                text = driver.find_element(By.CLASS_NAME, 'sri_btn_expired_apply').text
            else:
                text = ''
        except:
            print(f'class_name을 차지 못했습니다. :{url}')
            text = '없어!'

        # 회사+공고명 중복 제거용 키
        combined = re.sub(r'\(?주식회사\)?|\(?주\)?|\(?유\)?', '', row['company_name']).strip() + ' ' + row['job_title'].strip()

        if text in stop_words:
            bad.append(row)
        elif combined not in seen:
            good.append(row)
            seen.add(combined)

        print(f"[GOOD] {source_counts}")

        # 각 사이트 3개 이상이면 끝 
        if all(count >= 3 for count in source_counts.values()):
            print("[✅] 사이트별로 3개씩 수집 완료. 중단합니다.")
            break

    driver.quit()
    bad_df = pd.DataFrame(bad)
    good_df = pd.DataFrame(good)

    return bad_df, good_df

# sendgrid 메일 발송 코드
def sg_mail(sender_email, recipient_email, subject, html_body):
    sg_api = os.getenv('SENDGRID_API_KEY')
    
    message = Mail(
        from_email=f'Jobdori <{sender_email}>',  # SendGrid에 등록한 이메일
        to_emails = recipient_email,                 # 아무 이메일 주소
        subject = subject,
        html_content = html_body
    )
    try:
        sg = SendGridAPIClient(sg_api)
        response = sg.send(message)
        print(f"✔️ Status Code: {response.status_code}")
    except Exception as e:
        print("❌ 오류 발생:", e)

# 메일 보내는 함수를 사용 / 교육생에게 보냄
def send_mail(name, email, course, very_good_df, job_sites):
    site_tr = "" 
    total_job_postings = 0
    for site in job_sites:
        job_tr = ""
        kr_name = site_name_kr.get(site)
        
        df_ = very_good_df[very_good_df['source_table'] == site].head(3)

        # 채용공고가 없는 사이트 제외
        if df_.empty:
            continue

        for idx, row in df_.iterrows():
            open_source = 'email'
            company = row['company_name']
            deadline = row['deadline']
            title = row['job_title']
            job_url = row['recruit_url']
            # 공고 정리
            encoded_url = urllib.parse.quote(job_url, safe='/')
            job_url = f"{api_gateway_url}?user_email={email}&user_id={name}&clicked_url={encoded_url}&course_id={course}&open_source={open_source}"
            # 각 채용공고 별 html 모으기
            job_tr += html_job_tr.format(job_url,company,deadline,title)
            total_job_postings += 1
        # 채용공고 사이트 별 table html
        site_tr += html_site_tr.format(kr_name, job_tr)
    
    today = datetime.today().strftime("%Y-%m-%d")
    
    # 최종 html
    html_res = html_header_tr.format(today, name, total_job_postings, site_tr)

    sender_email = 'jshyjh9129@gmail.com'
    
    recipient_email = email

    subject = f'📌 [JobDori 채용공고] {name}님! 이번주 {course} 채용공고는?'


    sg_mail(sender_email, recipient_email, subject, html_res) 

def save_send_job(final_df):
    db_user = os.getenv("Job_db_user")
    db_password = os.getenv("Job_db_password")
    db_host = os.getenv("Job_db_host")
    db_port = int(os.getenv("Job_db_port", 3306))
    db_name = "job"

    engine = create_engine(
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    final_df.to_sql(name="send_job", con=engine, if_exists='append', index=False)
    
    print('보낸 채용공고 저장')

# 메인 코드 
def main(df_students):
    # 최종 공고 
    good_job_list = []
    

    courses = ['Data Analyst', 'Data Engineer', 'Data Scientist and AI', 'FULLSTACK', 'SERVICE' ,'CONTENT']
    # job_sites = ['saramin', 'wanted', 'jumpit', 'incruit']
    job_sites = ['saramin','jumpit']
    for course in courses:
        # 쿼리문으로 채용공고 데이터 가져오기
        good = keyword_query(course,job_sites)
        _, good_df = job_check(good)
        good_job_list.append(good_df)
    # 모든 과정 final 채용공고
    final_df = pd.concat(good_job_list, ignore_index=True)
    
    # 보낸 공고 확인
    save_send_job(final_df)
    
    for idx,row in df_students.iterrows():
        name =row['name']
        email = row['email']
        course = row['course']
        very_good_df = final_df[final_df['course'] == course]
        send_mail(name, email, course, very_good_df, job_sites)

# send_mail 함수랑 공고 보내는 main 코드 작성해야댐.


######################################################################################
######################################################################################
######################################################################################
###################################################################################### 


# 수신 거부 인원 send_email = 2 바꾸기

# 바 ("UnsubList의 사본")
json_file_path = "/home/jhs/job/gcp_send_job/GOOGLE_API/jhs-individual-project-d1db417cbac6.json" 
gc = gspread.service_account(json_file_path) 
spreadsheet_url = "https://docs.google.com/spreadsheets/d/11BxCMyoJzc-MDw-dInf_Bg6VhCz-TC0rOFJGkiYKi2Y/edit?gid=0#gid=0"
worksheet = gc.open_by_url(spreadsheet_url)
sheet = worksheet.worksheet("수신거부")
rows = sheet.get_all_values()

update_sql = """UPDATE job_member SET send_email = 2 WHERE email = %s"""
# conn = connect_to_lms()
# cursor = conn.cursor()

# email_list = []
# for idx, row in enumerate(rows[1:], start=2):  # 첫 번째 행(헤더) 제외
#     if len(row) > 2 and '/' in row[2]:
#         name, email = row[2].split('/')[0].strip(), row[2].split('/')[1].strip()
#         email_list.append(email)

# for email in email_list:
#     cursor.execute(update_sql, (email,))

# conn.commit()
# cursor.close()
# conn.close()

#######################################
# 인원 데이터 가져오기
# 서비스 계정 인증 파일 경로
SERVICE_ACCOUNT_FILE = '/home/jhs/job/gcp_send_job/GOOGLE_API/jhs-individual-project-d1db417cbac6.json'  # 파일 경로를 실제 경로로 수정하세요

# 사용 권한 범위 (스프레드시트 읽기/쓰기)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# 인증 및 클라이언트 생성
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

# Google Sheets 문서 ID 및 시트 이름
SPREADSHEET_ID = '1hQScinjsOf7uRk56LmE_IBK47TdaWek6bYplThLgXRs'
SHEET_NAME = '설문지 응답 시트1'  # 실제 시트 이름 (예: 'Sheet1', '시트1', '데이터' 등)

# 시트 열기
sh = gc.open_by_key(SPREADSHEET_ID)
worksheet = sh.worksheet(SHEET_NAME)

# 시트 데이터를 DataFrame으로 변환
data = worksheet.get_all_records()
df_students = pd.DataFrame(data)

df_students.columns = ['timestamp', 'name', 'email', 'course', 'Career', 'idea ']

course_mapping = {
    '데이터 분석가': 'Data Analyst',
    '데이터 엔지니어': 'Data Engineer',
    '데이터 사이언티스트 및 AI 엔지니어': 'Data Scientist and AI',
    '풀스택 개발자': 'FULLSTACK',
    '서비스 기획자': 'SERVICE',
    '콘텐츠 기획자': 'CONTENT'
}
df_students['course'] = df_students['course'].map(course_mapping)

print(df_students)


JOB_KEYWORDS = {
    "Data Analyst": [
        '데이터 분석가','데이터분석가','데이터 분석','데이터분석', '데이터 정제', '데이터정제', '데이터 처리', '데이터처리',
        '데이터 분석 매니저','데이터 시각화', 'tableau', 'Tableau''데이터 마이닝', '비즈니스 인텔리전스', 'SQL', 'R 분석','Data Analyst',
        'Data Visualization', 'Data Mining','DB관리', 'db관리','데이터분석','데이터처리','데이터관리','데이터마이닝','Data Architect','데이터 리터러시'],
    "Data Engineer": [
        '데이터 엔지니어','데이터엔지니어', '데이터 정제', '데이터정제', '데이터 처리', '데이터처리','데이터 관리','ETL 개발', 'SQL', 'Hadoop', 'Data Engineer',
        'ETL Developer', 'Big Data Engineer', 'Data Management', 'Data Mining','DBA', '빅데이터', 'DB관리', 'db관리', '데이터처리','데이터관리','데이터마이닝','데이터 리터러시'],
    "Data Scientist and AI": [
        '데이터 사이언티스트', '데이터사이언티스트', 'ai 기획', 'AI 기획', 'ai기획', '머신러닝', 'AI 엔지니어', '인공지능 엔지니어','딥러닝/머신러닝',
        'Data Scientist', 'Machine Learning', 'AI Engineer','딥러닝', '자연어 처리','Deep Learning Engineer', 'Natural Language Processing', 'NLP',
        'AI 모델','사이언티스트', '인공지능', 'LLM'],
    "FULLSTACK": [
        '프론트엔드', '프론트앤드' '백엔드', '백앤드', '웹 서비스', '모바일 서비스','풀스택', 'Java', '웹 개발자','모바일 개발자',
        '앱 개발자', 'API 개발자','DevOps 엔지니어', 'DevOps 개발자',
         'Front-End Developer', 'Front-End', 'Back-End'
        'Back-End Developer', 'Full Stack Developer', 'Mobile Developer', 'Cloud Engineer', 
        'API Developer', 'DevOps Engineer', '웹 퍼블리셔','Vue.js', 
        'Node.js', 'Frontend Engineer', 'Backend Engineer', 'React Developer', 'Node.js Developer', 'Vue.js Developer', 'CI/CD'
    ],
    "SERVICE": [
    '서비스 기획', '서비스기획', '프로덕트 매니저', '프로덕트매니저', '서비스 기획자','서비스기획자', '서비스 기획 매니저', '서비스기획 매니저', '서비스 기획 PM', 
    '서비스 기획 PL','서비스기획 PM', '서비스기획 PL', '서비스 기획 담당자', '서비스기획 담당자', 'product manager','PRODUCT MANAGE', 'PRODUCT MANAGER',
    'product manage', '서비스 전략', '서비스 운영', '서비스 플래너', '서비스 디렉터','서비스전략', '서비스운영', '서비스플래너', '서비스디렉터', '고객 경험', 'UX 기획', '사용자 시나리오'
],
    "CONTENT": [
        '콘텐츠 기획', '콘텐츠기획', '콘텐츠 기획자', '콘텐츠기획자', '콘텐츠 에디터', '콘텐츠 전략', '콘텐츠 디렉터', '콘텐츠 매니저', '콘텐츠 담당자', '콘텐츠에디터', '콘텐츠전략',
        '콘텐츠디렉터', '콘텐츠매니저', '콘텐츠담당자', '콘텐츠 제작', '콘텐츠 구성', '콘텐츠제작', '콘텐츠구성', '브랜드 콘텐츠', '마케팅 콘텐츠', '브랜드콘텐츠', '마케팅콘텐츠',
        '서비스 기획', '서비스기획', '프로덕트 매니저', '프로덕트매니저', '서비스 기획자', '서비스기획자', '서비스 기획 매니저', '서비스 기획 PM', '서비스 기획 PL',
        '서비스 기획 담당자', '교육 콘텐츠 기획', 'product manage', 'product manager', 'PRODUCT MANAGE', 'PRODUCT MANAGER'
]
}

STOPWORDS = {
    "Data Analyst": [
    '석사', '박사', '교육생', '국비', '청년수당', '코드잇', '제약', '건설', '연구', '기계', '행정', '호텔',
    '천재교육', '천재교과서', 'SeSAC', 'APM', 'apm', 'EDI', 'edi', '양성', '센터', '마케터', '경력직', '경력자'
],
    "Data Engineer": [
    '석사', '박사', '교육생', '국비', '청년수당', '코드잇', '제약', '건설', '연구', '기계', '행정', '호텔',
    '천재교육', '천재교과서', 'SeSAC', 'APM', 'apm', 'EDI', 'edi', '양성', '센터', '마케터'
],
    "Data Scientist and AI": [
    '석사', '교육생', '국비', '청년수당', '코드잇', '제약', '건설', '연구', '기계', '행정', '호텔',
    '천재교육', '천재교과서', 'SeSAC', 'APM', 'apm', 'EDI', 'edi', '양성', '센터', '마케터', '경력직', '경력자'
],
    "FULLSTACK": [
        '석사', '박사', '코드잇', '교육생', '국비', '청년수당', '제약', '건설', '연구', '기계', '행정', '호텔', '천재교육', 
    '천재교과서', 'SeSAC', 'APM', 'apm', 'edi', 'EDI', 'UX', 'UI', '마케터','양성','기획', '경력직', '경력자'
    ],
    "SERVICE": [
        '창업', '번역', '석사', '박사', '코드잇', '교육생', '국비', '청년수당', '제약', '건설', '연구', '기계', '행정', '호텔',
         '천재교육', '천재교과서', 'SeSAC', 'APM', 'apm', 'edi', 'EDI', '개발자', '시공','유튜브','양성', '경력직', '경력자'
    ],
    "CONTENT": [
        '창업', '번역', '석사', '박사', '코드잇', '교육생', '국비', '청년수당', '제약', '건설', '연구', '기계', '행정', '호텔',
         '천재교육', '천재교과서', 'SeSAC', 'APM', 'apm', 'edi', 'EDI', '개발자', '시공','유튜브','양성', '경력직', '경력자'
    ]
}

main(df_students)