import os
import json
import pandas as pd
import sqlalchemy as sa
from openai import OpenAI
from nltk import download
from nltk.corpus import stopwords


def drop_tbl(tbl_name):
    query = f"""
        DROP TABLE IF EXISTS {tbl_name}
        """
    return query


def table_to_sqltbl(base_tbl, sql_tbl_name, conn):
    base_tbl.to_sql(f'{sql_tbl_name}', conn, if_exists="replace", index=False, schema='online')


def process_completion(client, prompt, json_format):
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
    except json.decoder.JSONDecodeError:
        print(f'API call failure: Prompt: {prompt} \n Completion: {completion}')
        return None
    
    json_name = list(json.loads(event).keys())[0]

    output_json = json.loads(event)[f'{json_name}']
    output_table = pd.DataFrame(pd.json_normalize(output_json))

    return output_table

def print_result(completed, remaining, failed, flush=True):
    print(f'Completed: {completed}. Remaining: {remaining}. Failed: {failed}', end='\r', flush=flush)


def create_temp_headers(conn, temptblname, basetblname):
    conn.execute(sa.text(f"""SELECT TOP(0) * INTO {temptblname} from {basetblname}"""))


def establish_connection(API_KEY, sql_user, sql_pass, sql_server, sql_db, sql_driver):
    client = OpenAI(api_key=API_KEY)
    connection_url = f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_server}/{sql_db}?driver={sql_driver}"
    engine = sa.create_engine(connection_url)
    return client, engine
    

def get_stops():
    download('stopwords')

    __location__ = os.path.realpath(
                os.path.join(os.getcwd(), os.path.dirname(__file__)))

    allowed_stops_file = open(os.path.join(__location__, r'AllowedStopWords.csv'))
    allowed_stops = pd.read_csv(allowed_stops_file)['AllowedWords'].tolist()

    stops = [s for s in stopwords.words('english') if s not in allowed_stops]
    return stops