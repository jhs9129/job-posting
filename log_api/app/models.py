# ▶ app/models.py
from pydantic import BaseModel

class ClickLog(BaseModel):
    user_email: str
    user_id: str
    clicked_url: str
    course_id: str
    open_source: str