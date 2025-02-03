from fastapi import FastAPI
from s_base_db import DatabaseSession

app = FastAPI()
db_session = DatabaseSession()

@app.get('/')
def run_query():
    try:
        result = db_session.execute_query("select * from meme where name = %s limit 2;",("sarcasm",))
        return result
    except Exception as e:
        return e