import os
import json
import pandas as pd
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.engine.base import Connection, Engine

from openai import OpenAI
import threading

from nltk import download
from nltk.corpus import stopwords


def drop_tbl_query(tbl_name: str) -> str:
    query = f"""
        DROP TABLE IF EXISTS {tbl_name}
        """
    return query


def table_to_sqltbl(base_tbl: pd.DataFrame, sql_tbl_name: str, conn: Connection):
    base_tbl.to_sql(f'{sql_tbl_name}', conn, if_exists="replace", index=False, schema='online')


def process_completion(client: OpenAI, prompt: str, json_format) -> pd.DataFrame:
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ],
        response_format=json_format,
        temperature=0
        # max_tokens=500
        )

    try:
        event = fr"""{completion.choices[0].message.content}"""
    except json.decoder.JSONDecodeError as e:
        errmsg = f"""
            JSON decode failure: Prompt: {prompt}
            Completion: {completion}. 
            """
        e.add_note(errmsg)
        raise 
        
    json_name = list(json.loads(event).keys())[0]

    output_json = json.loads(event)[f'{json_name}']
    output_table = pd.DataFrame(pd.json_normalize(output_json))

    return output_table


def print_thread_count():
    print(f'Active Threads: {threading.active_count()}', end='\r', flush=True)


def print_result(completed: int, remaining: int, failed: int, flush: bool):
    print(f'Completed: {completed}. Remaining: {remaining}. Failed: {failed}',
           end='\r',
           flush=flush
        )


def create_temp(conn: Connection, temptblname: str, basetblname: str):
    drop_tbl_query(temptblname)
    conn.execute(sa.text(f"""SELECT * INTO {temptblname} from {basetblname}"""))


def establish_connection(
        API_KEY: str,
        sql_user: str,
        sql_pass: str,
        sql_server: str,
        sql_db: str,
        sql_driver: str
        ) -> tuple[OpenAI, Engine]:
    
    client = OpenAI(api_key=API_KEY)
    connection_url = f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_server}/{sql_db}?driver={sql_driver}"
    engine = sa.create_engine(
        connection_url,
        pool_size = 15,
        max_overflow = 30,
        pool_timeout=30,
        pool_pre_ping=True)
    
    return client, engine
    

def get_stops() -> list:
    download('stopwords')

    __location__ = os.path.realpath(
                os.path.join(os.getcwd(), os.path.dirname(__file__)))

    allowed_stops_file = open(os.path.join(__location__, r'AllowedStopWords.csv'))
    allowed_stops = pd.read_csv(allowed_stops_file)['AllowedWords'].tolist()

    stops = [s for s in stopwords.words('english') if s not in allowed_stops]
    return stops