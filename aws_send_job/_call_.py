# from dotenv import load_dotenv
import os
import mariadb
import logging

# 채용공고 
def connect_to_job():
    try:
        connection = mariadb.connect(host = os.getenv('Job_db_host'),
                                             user = os.getenv('Job_db_user'),
                                             password = os.getenv('Job_db_password'),
                                             db = os.getenv('Job_db_name'),
                                             port=int(os.getenv('Job_db_port', 3306))
                                             )
        return connection
    except mariadb.Error as err:
        return None



# 학관시
def connect_to_lms():
    try:
        connection = mariadb.connect(host = os.getenv('LMS_db_host'),
                                            user = os.getenv('LMS_db_user'),
                                            #  password = 'gkals123!',
                                             password = os.getenv('LMS_db_password'),
                                             db = os.getenv('LMS_db_name'),
                                             port=int(os.getenv('LMS_db_port', 7000))
                                             )
        return connection   
    except mariadb.Error as err: 
        return None

# html 읽는 함수
def read_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
    return html_content



# 쿼리 읽는 함수
def get_sql_query_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            sql_query = file.read()
        logging.info(f"Successfully read SQL query from {file_path}")
        return sql_query
    except Exception as e:
        logging.error(f"Error reading SQL query from file: {e}")
        return None