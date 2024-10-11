
import pandas as pd

import sqlalchemy as sa
from credentials.SQL_Credentials import username, password, server, database, driver

from openai import OpenAI
from credentials.OpenAI_API_Key import API_KEY

import tools.summarytools as summtools
import tools.aitools as aitools

client, engine = aitools.establish_connection(API_KEY, username, password, server, database, driver)

YEAR_MONTH = 'SEP-24'

MMM_YYtoDate = f"""
            SELECT ActualDate FROM Dates WHERE ActualDate = CONVERT(DATE, CONCAT('01-', '{YEAR_MONTH}'))
            """

def process_summaries(obj, conn, date_int, date_string):
    completed = 0
    failed = 0
    offset = 0
    try_count = 1    
    while True:
        
        remaining = obj.get_count_remaining(conn, date_int, date_string) - failed
      
        if remaining == 0 : 
            print(f'All {obj.table}s Summarised For {YEAR_MONTH.title()}.')
            break
        
        # Grab all non-null ReviewText reviews for a venue. 
        input_tbl = obj.get_remaining_rows(conn, date_int, date_string, offset)

        if obj.__class__.__name__ == 'VenueSummary': 
            # retain only alpha chars
            input_tbl.ReviewText = input_tbl.ReviewText.str.replace('[^a-zA-Z ]', '', regex=True).str.strip()
            input_json = input_tbl.ReviewText.to_json(orient='records')
            
        else: 
            # feed venue summaries and create operator summaries from them
            input_json = input_tbl["VenueSummary"].to_json(orient='records')


        FK_ID = None
        unknown_region = None
        if obj.__class__.__name__ == 'RegionSummary':
            FK_ID = summtools.get_unique_ids(input_tbl, obj.foreign_key)
            unknown_region = summtools.getid_fromvalue(obj, '-', conn)

        PK_ID = summtools.get_unique_ids(input_tbl, obj.primary_key)


        # if Region unknown, do not summarise by region.
        if PK_ID == unknown_region:
            output_table = pd.DataFrame({'Summary': ['-']})
        else:
            prompt = summtools.summary_prompt(input_json, obj.table)
            output_table = aitools.process_completion(client, prompt, summtools.JSON_FORMAT)

    
        # check valid summary length.
        summary = output_table['Summary'].to_list()
        summary_wordlen = len(summary[0].split(' '))

        if try_count != 3:
            if summary_wordlen <= 50:
                conn.execute(sa.text(aitools.drop_tbl('#temp')))
                aitools.table_to_sqltbl(base_tbl=output_table, sql_tbl_name='#temp', conn=conn)

                conn.execute(sa.text(obj.temp_insert(date_string, PK_ID, FK_ID)))
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
        aitools.print_result(completed, remaining, failed, flush)


def main():
    with engine.begin() as conn:
        dateid_tbl = pd.read_sql(sa.text(MMM_YYtoDate), conn)
        date_string = str(dateid_tbl['ActualDate'][0])

        dateid_intquery = f"SELECT DateID FROM Dates d where d.ActualDate = '{date_string}'"
        dateid_tbl = pd.read_sql(sa.text(dateid_intquery), conn)
        date_int = int(dateid_tbl['DateID'][0])

        aitools.create_temp(conn, '#SummaryVenue', 'Summary_Venue')
        aitools.create_temp(conn, '#SummaryOperator', 'Summary_Operator')
        aitools.create_temp(conn, '#SummaryRegion', 'Summary_Region')

        process_summaries(summtools.VenueSummary(), conn, date_int, date_string)
        process_summaries(summtools.OperatorSummary(), conn, date_int, date_string)
        process_summaries(summtools.RegionSummary(), conn, date_int, date_string)

        conn.execute(sa.text(summtools.final_insert()))

if __name__ == '__main__':
    main()