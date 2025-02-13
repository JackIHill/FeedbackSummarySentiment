
import pandas as pd

import sqlalchemy as sa
from sqlalchemy.engine.base import Connection

from credentials.SQL_Credentials import username, password, server, database, driver

from credentials.OpenAI_API_Key import API_KEY

import tools.summarytools as summtools
import tools.aitools as aitools

import os

DEFAULT_NUM_WORKERS = os.cpu_count() * 5
client, engine = aitools.establish_connection(API_KEY, username, password, server, database, driver, DEFAULT_NUM_WORKERS)


def process_summaries(obj, conn: Connection, rep_MMM_YY: str, num_months: int):

    start_date_id, end_date_id = aitools.start_end_date(rep_MMM_YY, num_months)
    conn.execute(sa.text(summtools.date_range_insert(start_date_id, end_date_id, num_months)))
    
    completed = 0
    failed = 0
    offset = 0
    try_count = 1    

    while (obj.table_name == 'Venue' and completed <= 5) or (obj.table_name != 'Venue'):
        remaining = obj.get_count_remaining(conn, start_date_id, end_date_id) - failed
      
        if remaining == 0: 
            print(f'All {obj.table_name}s Summarised For {rep_MMM_YY.title()}.')
            break
        
        # Grab all non-null ReviewText reviews for a venue. 
        input_tbl = obj.get_remaining_rows(conn, offset, start_date_id, end_date_id)


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
            prompt = summtools.summary_prompt(input_json, obj.table_name)
            output_table = aitools.process_completion(client, prompt, summtools.JSON_FORMAT)

            # print()
            # print("VenueID: "+ str(summtools.get_unique_ids(input_tbl, 'VenueID')), "Count Reviews: " + str(len(input_tbl)))
            # pd.set_option('display.max_colwidth', None)
            # print(output_table['Pros'].to_string())
            # print(output_table['Cons'].to_string())
            # print()

            # replace None etc with '-'
        
        
        # check valid summary length.
        summary = output_table['Summary'].to_list()
        summary_wordlen = len(summary[0].split(' '))

        if try_count != 3 and len(output_table.index) == 1:
            if summary_wordlen <= 50:
                conn.execute(sa.text(aitools.drop_tbl_query('#temp')))
                aitools.table_to_sqltbl(base_tbl=output_table, sql_tbl_name='#temp', conn=conn)

                conn.execute(sa.text(obj.temp_insert(start_date_id, end_date_id, PK_ID, FK_ID)))
                completed += 1
            else:
                try_count += 1
                continue

        elif try_count == 3:
            failed += 1
            # skip the batch if failed 3x
            offset += 1

        try_count = 1

        aitools.print_result(1, completed, remaining, failed)


def main(rep_MMM_YY: str, num_months: int):
    with engine.begin() as conn:

        aitools.create_temp(conn, '#SummaryVenue', 'Summary_Venue')
        aitools.create_temp(conn, '#SummaryOperator', 'Summary_Operator')
        aitools.create_temp(conn, '#SummaryRegion', 'Summary_Region')
        aitools.create_temp(conn, '#DateRange', 'DateRange')

        process_summaries(summtools.VenueSummary(), conn, rep_MMM_YY, num_months)
        process_summaries(summtools.OperatorSummary(), conn, rep_MMM_YY, num_months)
        process_summaries(summtools.RegionSummary(), conn, rep_MMM_YY, num_months)

        conn.execute(sa.text(summtools.final_insert()))

if __name__ == '__main__':
    main('FEB-25', 12)