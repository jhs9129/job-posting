import boto3
from botocore.exceptions import ClientError
import mysql.connector
import json
import os
import logging
import re
import mariadb
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

from _call_ import * 

load_dotenv()

# selenium 설정
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")


# html 불러오기
html_header_tr = read_html_file('./HTML_CODE/header.html')
html_site_tr = read_html_file('./HTML_CODE/site_tr.html')
html_job_tr = read_html_file('./HTML_CODE/job_tr.html')

# api_gateway 
api_gateway_url = "https://n5oeg83wll.execute-api.ap-northeast-2.amazonaws.com/active/jobclick"

site_name_kr = {'saramin': '사람인','wanted': '원티드','jumpit': '점핏','incruit': '인크루트'}


# 포함, 제외 키워드 정하기
def create_job_conditions(course):
    course_upper = course.upper()
    if "BIGDATA" in course_upper:
        return JOB_KEYWORDS["BIGDATA"], STOPWORDS["BIGDATA"]
    elif "FULLSTACK" in course_upper:
        return JOB_KEYWORDS["FULLSTACK"], STOPWORDS["FULLSTACK"]
    elif "PM" in course_upper:
        return JOB_KEYWORDS["PM"], STOPWORDS["PM"]
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
            'wanted': 0,
            'jumpit': 0,
            'incruit': 0
        }
        for g in good:
            s = g['source_table']
            if s in source_counts:
                source_counts[s] += 1

        # 만약 현재 사이트의 공고가 이미 20개면 건너뜀
        if source_counts.get(source, 0) >= 20:
            print(f"[PASS]-- [{source}] 20개 초과, 건너뜀: {row['company_name']} - {row['job_title']}")
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
            elif 'wanted' in url:
                text = driver.find_element(By.CLASS_NAME, 'WantedApplyBtn_container__lBx_L').text
            elif 'incruit' in url:
                text = driver.find_element(By.CLASS_NAME, 'btn_bgrp.btn_jobapp.off').text
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
        if all(count >= 7 for count in source_counts.values()):
            print("[✅] 사이트별로 7개씩 수집 완료. 중단합니다.")
            break

    driver.quit()
    bad_df = pd.DataFrame(bad)
    good_df = pd.DataFrame(good)

    return bad_df, good_df

# ses 메일 발송 코드
def ses_mail(sender_email, recipient_email, subject, html_body):
    aws_region = os.getenv('aws_region')
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    
    # ses_client 생성
    ses_client = boto3.client('ses',
                              region_name = aws_region,
                              aws_access_key_id = aws_access_key_id,
                              aws_secret_access_key = aws_secret_access_key
                              )
    
    # 'chunjae'라는 문자열이 recipient_email에 포함된 경우 ConfigurationSetName을 생략
    config_set = None  # 기본적으로 Configuration Set을 사용하지 않도록 설정

    if 'chunjae' in recipient_email:  # 제목에 'chunjae'가 포함되어 있으면
        config_set = None  # ConfigurationSetName을 None으로 설정
    else:
        config_set = 'observe'  # 그렇지 않으면 'observe'라는 Configuration Set을 사용
    
    try:
        response = ses_client.send_email(
            Destination = {
                'ToAddresses': [recipient_email],
            },
            Message = {
                'Body' : {
                    'Html' : {
                        'Charset' : 'UTF-8',
                        'Data' : html_body
                    },
                },
                'Subject' : {
                    'Charset' : 'UTF-8',
                    'Data' : subject
                },
            },
            **({"ConfigurationSetName": config_set} if config_set else {}),
            Source = f'"천재IT교육센터" <{sender_email}>',
        )
        logging.info(f"Email sent successfully to {recipient_email}")
    except ClientError as e:
        logging.error(f"Error sending email: {e.response['Error']['Message']}")
    else:
        logging.info("Email sent successfully")



# 메일 보내는 함수를 사용 / 교육생에게 보냄
def send_mail(name, email, course, very_good_df, job_sites):
    nomal_tr = "" 
    job_tr = ""
    total_job_postings = 0

    # 보내는 공고 모음집(db에 적재하려고)
    sent_df = pd.DataFrame(columns=very_good_df.columns)
    for site in job_sites:
        # kr_name = site_name_kr.get(site)
        
        # 사람인으로 못채운 갯수 채우려고 사람인은 다 가져옴(총 20개)
        if site =='saramin':
            df_ = very_good_df[very_good_df['source_table'] == site].head(20-total_job_postings)
        else:
            df_ = very_good_df[very_good_df['source_table'] == site].head(5)

        # # 채용공고가 없는 사이트 제외
        # if df_.empty:
        #     continue

        for idx, row in df_.iterrows():
            company = row['company_name']
            deadline = row['deadline']
            title = row['job_title']
            job_url = row['recruit_url']
            open_source = 'mail'
            # 공고 정리
            encoded_url = urllib.parse.quote(job_url, safe='/')
            job_url = f"{api_gateway_url}?user_id={email}&name={name}&url={encoded_url}&course={course}&open_source={open_source}"
            # 각 채용공고 별 html 모으기
            job_tr += html_job_tr.format(job_url,company,deadline,title)
            total_job_postings += 1

        # 보내는 공고 누적
        sent_df = pd.concat([sent_df, df_])

        # 채용공고 사이트 별 table html
    nomal_tr += html_site_tr.format('🔎 채용 중인 과정 추천 공고', job_tr)
    
    today = datetime.today().strftime("%Y-%m-%d")
    
    # 최종 html
    html_res = html_header_tr.format(today, name, total_job_postings, nomal_tr)

    sender_email = 'chunjaecloud@gmail.com'
    
    recipient_email = email

    subject = f'📌 [천재IT교육센터] {name}님! 이번주 {course} 채용공고는?'
    feedback_ = '📌 [천재IT교육센터] 채용공고 피드백 요청' 

    ses_mail(sender_email, recipient_email, subject, html_res) 
    
    # 보낸 공고 db에 적재
    db_user = os.getenv("Job_db_user")
    db_password = os.getenv("Job_db_password")
    db_host = os.getenv("Job_db_host")
    db_port = int(os.getenv("Job_db_port", 3306))
    db_name = "crawling"

    engine = create_engine(
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    # 이미 DB에 있는 recruit_url 불러오기
    existing_urls = pd.read_sql("SELECT recruit_url FROM send_job", con=engine)
    existing_urls_set = set(existing_urls['recruit_url'])

    # 중복 제거
    sent_df = sent_df[~sent_df['recruit_url'].isin(existing_urls_set)]

    # 공고가 있을 때만 적재
    if not sent_df.empty:
        sent_df.to_sql(name="send_job", con=engine, if_exists="append", index=False)

def save_db(final_df):
    db_user = os.getenv("Job_db_user")
    db_password = os.getenv("Job_db_password")
    db_host = os.getenv("Job_db_host")
    db_port = int(os.getenv("Job_db_port", 3306))
    db_name = "crawling"

    engine = create_engine(
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    print('save start')

    final_df.to_sql(name="send_job", con=engine, if_exists='append', index=False)
    
    print('save end')


# 메인 코드 
def main(df_students):
    # 최종 공고 
    good_job_list = []
    
    courses = ['BIGDATA', 'FULLSTACK', 'PM']
    # 마지막에 사람인으로 앞 3개 사이트의 부족한 공고를 매꿈 그래서 꼭 사람인이 마지막이여야 함.
    job_sites = ['wanted', 'jumpit', 'incruit','saramin']
    for course in courses:
        # 쿼리문으로 채용공고 데이터 가져오기
        good = keyword_query(course,job_sites)
        _, good_df = job_check(good)
        good_job_list.append(good_df)
    # 모든 과정 final 채용공고
    final_df = pd.concat(good_job_list, ignore_index=True)
    # 마감공고 및 이전에 보낸 공고 파악   

    save_db(final_df)
    
    # for idx,row in df_students.iterrows():
    #     name =row['name']
    #     email = row['email']
    #     course = row['subject']
    #     very_good_df = final_df[final_df['course'] == course]
    #     send_mail(name, email, course, very_good_df, job_sites)

# send_mail 함수랑 공고 보내는 main 코드 작성해야댐.


######################################################################################
######################################################################################
######################################################################################
###################################################################################### 


# 수신 거부 인원 send_email = 2 바꾸기

# 바 ("UnsubList의 사본")
json_file_path = "/home/ubuntu/job_posting/GOOGLE_API/genia_email-recommand-6976a7d469c3.json" 
gc = gspread.service_account(json_file_path) 
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1GdC3sv6q-t2v25alAmS83M76eDsrhfZBwTFOrd0Q1jw/edit?resourcekey=&gid=277815760#gid=277815760"
worksheet = gc.open_by_url(spreadsheet_url)
sheet = worksheet.worksheet("UnsubList의 사본")
rows = sheet.get_all_values()

update_sql = """UPDATE job_member SET send_email = 2 WHERE email = %s"""
conn = connect_to_lms_test()
cursor = conn.cursor()

email_list = []
for idx, row in enumerate(rows[1:], start=2):  # 첫 번째 행(헤더) 제외
    if len(row) > 2 and '/' in row[2]:
        name, email = row[2].split('/')[0].strip(), row[2].split('/')[1].strip()
        email_list.append(email)

for email in email_list:
    cursor.execute(update_sql, (email,))

conn.commit()
cursor.close()
conn.close()

#######################################

# lms 에서 채용공고 보낼 인원 파악 
conn = connect_to_lms_test()
cursor = conn.cursor()

#* lms_test 바꿔야 함.
update_sql = """
UPDATE lms_test.job_member
LEFT JOIN lms.course ON lms_test.job_member.cno = lms.course.no
SET lms_test.job_member.send_email = 1
WHERE lms_test.job_member.send_email IN (0, 3)
  AND lms_test.job_member.status NOT IN ('REST', 'OUT','EMPLOYED')
  AND DATEDIFF(CURDATE(), lms.course.start_date) >= 140; 
"""
cursor.execute(update_sql)
conn.commit()

#* lms_test 바꿔야 함.
# 2. send_email이 1인 사람들만 조회
select_sql = """
SELECT  
    lms_test.job_member.*, 
    lms.course.start_date, 
    lms.course.end_date,
    lms.course.subject, 
    lms.course.flag   
FROM lms_test.job_member
LEFT JOIN lms.course ON lms_test.job_member.cno = lms.course.no
WHERE lms_test.job_member.send_email = 1
AND lms_test.job_member.role = 'student';
"""

cursor.execute(select_sql)
columns = [column[0] for column in cursor.description]
df_students = pd.DataFrame(cursor.fetchall(), columns=columns)
print(df_students)
cursor.close()
conn.close()



# 과정 별 키워드 및 불용어
JOB_KEYWORDS = {
    "BIGDATA": [
        '데이터 사이언티스트', '데이터 엔지니어', '데이터 분석가', '데이터사이언티스트', '데이터엔지니어', '데이터분석가',
        '데이터 분석','데이터분석', '데이터 정제', '데이터정제', '데이터 처리', '데이터처리', 'ai 기획', 'AI 기획', 'ai기획',
        '데이터 관리', '데이터 분석 매니저', '머신러닝', 'AI 엔지니어', '인공지능 엔지니어', '데이터 시각화', 'tableau', 'Tableau'
        '데이터 마이닝', '비즈니스 인텔리전스', 'ETL 개발', 'SQL', 'R 분석', 'Hadoop', '에듀테크 콘텐츠 개발', '딥러닝/머신러닝',
        'Data Scientist', 'Data Engineer', 'Data Analyst', 'Machine Learning', 'AI Engineer', 'Data Visualization', 
        'Business Intelligence', 'ETL Developer', 'Big Data Engineer', 'Data Management', 'Data Mining', '딥러닝', '자연어 처리',
        'Deep Learning Engineer', 'Natural Language Processing', 'NLP', 'DBA', '빅데이터' 'AI 모델','DB관리', 'db관리',
        '사이언티스트', '인공지능', '데이터분석','데이터처리','데이터관리','데이터마이닝', 'LLM','Data Architect','데이터 리터러시'],
    "FULLSTACK": [
        '프론트엔드', '프론트앤드' '백엔드', '백앤드', '웹 서비스', '모바일 서비스','풀스택', 'Java', '웹 개발자',
        '소프트웨어 엔지니어', '소프트웨어 개발자', '시스템 엔지니어', '시스템 개발자', '모바일 개발자',
        '앱 개발자', 'API 개발자', '클라우드 엔지니어', 'DevOps 엔지니어', 'DevOps 개발자', '서버 개발자',
        '네트워크 엔지니어', '네트워크 개발자', 'Front-End Developer', 'Front-End', 'Back-End'
        'Back-End Developer', 'Full Stack Developer', 'Software Engineer', 'Mobile Developer', 'Cloud Engineer', 
        'API Developer', 'DevOps Engineer', 'Server Developer', 'Web Designer', '웹 퍼블리셔','Vue.js', 
        'Node.js', 'Frontend Engineer', 'Backend Engineer', 'React Developer', 'Node.js Developer', 'Vue.js Developer', 'CI/CD'
    ],
    "PM": [
        '서비스 기획', '서비스기획', '프로덕트 매니저', '프로덕트매니저', '서비스 기획자','서비스기획자','서비스 기획 매니저', '서비스 기획 PM', 
        '서비스 기획 PL', '서비스 기획 담당자', '콘텐츠 기획','콘텐츠기획','콘텐츠기획자','콘텐츠 기획자' '교육 콘텐츠 기획','product manage',
        'product manager', 'PRODUCT MANAGE', 'PRODUCT MANAGER','서비스 운영 매니저','서비스 운영 기획', '디지털 기획자', '사업 기획', '플랫폼 매니저',
        '프로덕트 오너', '프로젝트 기획', 'UX 기획자', 'UI 기획자', 'PRODUCT OWNER' , 'Product Owner', 'product owner'
    ]
}

STOPWORDS = {
    "BIGDATA": [
        '석사', '박사', '코드잇', '교육생', '국비', '청년수당', '제약', '건설', '연구', '기계', '행정', '호텔', '천재교육', 
    '천재교과서', 'SeSAC', 'APM', 'apm', 'edi', 'EDI', '마케터', '양성', '기획', '센터' ,'경력직', '경력자'
    ],
    "FULLSTACK": [
        '석사', '박사', '코드잇', '교육생', '국비', '청년수당', '제약', '건설', '연구', '기계', '행정', '호텔', '천재교육', 
    '천재교과서', 'SeSAC', 'APM', 'apm', 'edi', 'EDI', 'UX', 'UI', '마케터','양성','기획', '경력직', '경력자'
    ],
    "PM": [
        '창업', '번역', '석사', '박사', '코드잇', '교육생', '국비', '청년수당', '제약', '건설', '연구', '기계', '행정', '호텔',
         '천재교육', '천재교과서', 'SeSAC', 'APM', 'apm', 'edi', 'EDI', '개발자', '시공','유튜브','양성', '경력직', '경력자',
         '물류', '전기차' ,
    ]
}


main(df_students)