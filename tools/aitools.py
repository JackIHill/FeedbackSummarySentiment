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

import logging
from pathlib import Path

def drop_tbl_query(tbl_name: str) -> str:
    query = f"""
        DROP TABLE IF EXISTS {tbl_name}
        """
    return query


def table_to_sqltbl(
        base_tbl: pd.DataFrame,
        sql_tbl_name: str,
        idx_col_name: str,
        conn: Connection):
    
    base_tbl.to_sql(f'{sql_tbl_name}', conn, if_exists="replace", index=False, schema='online')
    conn.execute(
        sa.text(f"""CREATE INDEX idx ON {sql_tbl_name} ({idx_col_name})"""))
    

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


def print_thread_count(end: str = '\r'):
    # :<50 to add white space and prevent overlap with other console messages.
    print(f'Active Threads: {threading.active_count():<50}', end=end, flush=True)


def move_cursor_up():
    print('\033[F', end='', flush=True)


def print_result(num_rows: int, completed: int, remaining: int, failed: int, end: str = '\r', flush: bool = True):
    if (remaining < num_rows) or remaining == 0:
        end = '' 

    # :<50 to add white space and prevent overlap with other console messages.
    print(f'Completed: {completed}. Remaining: {remaining}. Failed: {failed:<50}',
           end=end,
           flush=flush
        )


def print_failed_review_err(current_offset: int, error: Optional[str] = None):
    output = f"\nFailed to process reviews at offset {current_offset}"
    if error:
        output += f': {error}'

    print(output)


def create_temp(conn: Connection, temptblname: str, basetblname: str):
    drop_tbl_query(temptblname)
    conn.execute(sa.text(f"""SELECT * INTO {temptblname} from {basetblname}"""))


def establish_connection(
        API_KEY: str,
        sql_user: str,
        sql_pass: str,
        sql_server: str,
        sql_db: str,
        sql_driver: str,
        num_workers: int
        ) -> tuple[OpenAI, Engine]:
    
    client = OpenAI(api_key=API_KEY)
    connection_url = f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_server}/{sql_db}?driver={sql_driver}"
    engine = sa.create_engine(
        connection_url,
        pool_size = num_workers * 2,
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


def create_logger(filename: str = 'output.log', directory_name: Optional[str] = 'logs'):
    log_file_name = filename

    working_directory = Path(__file__).resolve().parent.parent

    if directory_name:
        path = f'{working_directory}\{directory_name}'
        os.makedirs(path, exist_ok=True)

    logging.basicConfig(
        filename=f'{path if directory_name else working_directory}\{log_file_name}',
        format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %H:%M:%S',
        level=logging.INFO,
        )
 
    logger = logging.getLogger(__name__)

    # turn off OpenAI request logs
    logging.getLogger("httpx").setLevel(logging.WARNING)

    return logger

