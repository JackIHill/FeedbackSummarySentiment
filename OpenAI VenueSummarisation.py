
import pandas as pd
from nltk import download
from nltk.corpus import stopwords

import sqlalchemy as sa
from SQL_Credentials import username, password, server, database, driver

from openai import OpenAI
from OpenAI_API_Key import API_KEY

import SummaryTools as summtools
import AI_Tools

client = OpenAI(api_key=API_KEY)

connection_url = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
engine = sa.create_engine(connection_url)

download('stopwords')

YEAR_MONTH = 'SEP-24'

MMM_YYtoDate = f"""
            SELECT ActualDate FROM Dates WHERE ActualDate = CONVERT(DATE, CONCAT('01-', '{YEAR_MONTH}'))
            """

def process_summaries(obj, conn):
    completed = 0
    failed = 0
    offset = 0
    try_count = 1    
    while True:
        
        # Grab all non-null ReviewText reviews for a venue. 
        remaining = obj.get_count_remaining(conn, date_int, date_string)

        
        if remaining == 0:
            print(f'All {obj.table}s Summarised For {YEAR_MONTH.title()}.')
            break
        
        input_tbl = obj.get_remaining_rows(conn, date_int, date_string, offset)

        if obj.__class__.__name__ == 'VenueSummary': 
            # retain only alpha chars
            input_tbl.ReviewText = input_tbl.ReviewText.str.replace('[^a-zA-Z ]', '', regex=True).str.strip()
            input_json = input_tbl.ReviewText.to_json(orient='records')
            
        else: 
            # feed venue summaries and create operator summaries from them
            input_json = input_tbl["VenueSummary"].to_json(orient='records')


        id2 = None
        if obj.__class__.__name__ == 'RegionSummary':
            id2 = summtools.get_unique_ids(input_tbl, summtools.OperatorSummary().idcol)

        id = summtools.get_unique_ids(input_tbl, obj.idcol)

        prompt = summtools.summary_prompt(input_json, obj.table)

        output_table = AI_Tools.process_completion(client, prompt, summtools.JSON_FORMAT)
        
        # check valid summary length.
        summary = output_table['Summary'].to_list()
        summary_wordlen = len(summary[0].split(' '))

        if try_count != 3:
            if summary_wordlen <= 50:
                conn.execute(sa.text(AI_Tools.drop_tbl('#temp')))
                AI_Tools.table_to_sqltbl(base_tbl=output_table, sql_tbl_name='#temp', conn=conn)

                conn.execute(sa.text(obj.temp_insert(date_string, id, id2)))
                completed += 1
            else:
                try_count += 1
                continue

        elif try_count == 3:
            failed += 1
            # skip the batch if failed 3x
            offset += 1

        try_count = 1

        flush = False if remaining <= 1 else True
        AI_Tools.print_result(completed, remaining, failed, flush)


with engine.begin() as conn:
    dateid_tbl = pd.read_sql(sa.text(MMM_YYtoDate), conn)
    date_string = str(dateid_tbl['ActualDate'][0])

    dateid_intquery = f"SELECT DateID FROM Dates d where d.ActualDate = '{date_string}'"
    dateid_tbl = pd.read_sql(sa.text(dateid_intquery), conn)
    date_int = int(dateid_tbl['DateID'][0])


    AI_Tools.drop_tbl('#SummaryVenue')
    AI_Tools.create_temp_headers(conn, '#SummaryVenue', 'Summary_Venue')

    AI_Tools.drop_tbl('#SummaryOperator')
    AI_Tools.create_temp_headers(conn, '#SummaryOperator', 'Summary_Operator')

    AI_Tools.drop_tbl('#SummaryRegion')
    AI_Tools.create_temp_headers(conn, '#SummaryRegion', 'Summary_Region')
    

    process_summaries(summtools.VenueSummary(), conn)
    process_summaries(summtools.OperatorSummary(), conn)
    process_summaries(summtools.RegionSummary(), conn)
    
    conn.execute(sa.text(summtools.final_insert(conn)))
