from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from app.database import get_db_connection
from datetime import datetime
import logging
import pytz


logging.basicConfig(level=logging.INFO)

app = FastAPI()

@app.get("/log")
def log_click(
    user_email: str,
    user_id: str = None,
    clicked_url: str = "",
    course_id: str = "",
    open_source: str = ""
    ):

    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        sql = """
        INSERT INTO user_click_log (user_email, user_id, clicked_url, course_id, open_source, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (user_email, user_id, clicked_url, course_id, open_source, now_kst))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
    logging.info(clicked_url)
    # 로그 저장 후 해당 URL로 리다이렉트
    return RedirectResponse(url=clicked_url, status_code=302)
