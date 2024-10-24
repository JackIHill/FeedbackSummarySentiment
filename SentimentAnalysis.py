import sqlalchemy as sa
from sqlalchemy.engine.base import Connection # for type hints
import pyodbc

from credentials.SQL_Credentials import username, password, server, database, driver
from credentials.OpenAI_API_Key import API_KEY

import tools.sentimenttools as senttools
import tools.aitools as aitools

import threading
from queue import Queue
from concurrent import futures

from typing import Optional
import os
import time

stops = aitools.get_stops()

DEFAULT_NUM_ROWS: int = 200
MIN_REVIEW_DATEID: Optional[int] = None

# Shared lock and offset for thread-safe increment
update_lock = threading.Lock()
offset_lock = threading.Lock()
print_lock = threading.Lock()
read_barrier = threading.Barrier(os.cpu_count() * 4)

id_queue = Queue()

global_offset: int = 0
global_remaining: Optional[int] = None
global_completed: int = 0
global_failed: int = 0
global_printed: bool = False

client, engine = aitools.establish_connection(API_KEY, username, password, server, database, driver)


def update_global_counters(
        completed: int,
        failed: int,
        conn: Connection,
        print_status: bool = True):
    
    global global_completed, global_failed, global_remaining
    with update_lock:
        global_completed += completed
        global_failed += failed
        global_remaining = senttools.get_count_remaining(conn, MIN_REVIEW_DATEID)
 
    if print_status:
        aitools.print_result(DEFAULT_NUM_ROWS, global_completed, global_remaining, global_failed)


# should probably use queue for this and fill with values from 1 to num_rows?
def get_next_offset(increment_by: int = DEFAULT_NUM_ROWS) -> int:
    global global_offset
    with offset_lock:
        current_offset = global_offset
        global_offset += increment_by
    return current_offset


# make decorator?
def with_retry(conn: Connection, query: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            result = conn.execute(sa.text(query))
            return result
        except (sa.exc.OperationalError, pyodbc.Error) as e:
            if attempt < max_retries and 'deadlock' in str(e).lower():
                time.sleep(1)
            else:
                raise e

# with print_lock:
#     if not global_printed:
#         print()
#         global_printed = True
                    


def analyse_sentiment():
    aitools.print_thread_count()

    global global_printed
    global global_remaining

    client, engine = aitools.establish_connection(API_KEY, username, password, server, database, driver)

    completed = 0
    failed = 0
    try_count = 1
    num_rows = DEFAULT_NUM_ROWS

    review_temp_name = '#review_no_sentiment'
    with engine.connect() as conn:
        with conn.begin():
        # Drop and recreate the temporary table
        
            with print_lock:
                if not global_printed:
                    print(f'\nFetching Reviews...\r')
                    global_printed = True
                    aitools.move_cursor_up()

            with_retry(conn, aitools.drop_tbl_query(review_temp_name))
            with_retry(conn, senttools.insert_reviews(review_temp_name, MIN_REVIEW_DATEID))
            
            read_barrier.wait()
            
            print('Reviews fetched for all threads. Processing...', end='\r')


        while True:
            current_offset = get_next_offset()

            try:
                with conn.begin():
                    if global_remaining is None:
                        with update_lock:
                            if global_remaining is None:
                                global_remaining = senttools.get_count_remaining(conn, MIN_REVIEW_DATEID)

                    # Fetch rows for sentiment analysis
                    reviews = senttools.get_remaining_sentiment_rows(review_temp_name, current_offset, num_rows, conn)

                    if reviews.empty:
                        print(f"No more reviews to process at offset {current_offset}")
                        break

                    # raw_reviews = reviews['ReviewText']
                    
                    # move this processing (and insert_reviews())
                    # outside of loop to reduce db calls and processing time
                    # remove stop words and non-alpha characters
                    reviews.ReviewText = reviews.ReviewText.apply(
                                lambda x: ' '.join([word for word in x.split() if word not in (stops)])
                    ).str.replace('[^a-zA-Z ]', '', regex=True).str.strip()

                    review_json = reviews.to_json(orient='records')
                    prompt = senttools.sentiment_prompt(review_json, num_rows)

                    output_table = aitools.process_completion(client, prompt, senttools.JSON_FORMAT)

                    # Sentiment 10 -> 1
                    # Sentiment 5 -> 0
                    # Sentiment 0 -> -1
                    # Previously Unknown Sentiment of -1 now = -1.2. Set to '-' in SQL.
                    output_table["Sentiment"] = (output_table["Sentiment"] - 5) / 5
                    
                    inputIDs = reviews['ReviewID'].to_list()
                    outputIDs = output_table['ReviewID'].to_list()
                    # print(output_table['Sentiment'].to_list())

                    # ensure that no two threaads have updated the same id more than once.
                    # this should never happen due to offset increment. 
                    duplicate_found = any(i in list(id_queue.queue) for i in inputIDs)
                    if duplicate_found:
                        # skip batch or retry instead...
                        print('Duplicate IDs found!')
                        quit()
                    # Add IDs to the queue
                    for i in inputIDs:
                        id_queue.put(i)


                    output_sentiment = output_table['Sentiment'].to_list()
                    invalid_output_sentiment = [
                        x for x in set(output_sentiment) if not -1.2 <= x <= 1.0 or (x * 10) % 2 != 0
                        ]

                    sentiment_temp_name = '#temp'
                    scale_factor = 0.2
                    if try_count != 3:
                        with update_lock:
                            if inputIDs == outputIDs and not invalid_output_sentiment:
                                conn.execute(sa.text(aitools.drop_tbl_query(sentiment_temp_name)))
                                aitools.table_to_sqltbl(output_table, sentiment_temp_name, conn)
                                
                                conn.execute(sa.text(senttools.update_review_tbl_query(sentiment_temp_name)))
                                completed_rows = senttools.count_completed(sentiment_temp_name, conn)
                                
                                # input_reviewtext = str(raw_reviews.to_list())[1:-1]
                                # conn.execute(sa.text(senttools.delete_completed_from_temp(review_temp_name, input_reviewtext)))

                                completed += completed_rows
                                
                            else:
                                try_count += 1
                                num_rows = max(1, int(num_rows * (1 - scale_factor)))
                                continue

                    elif try_count == 3:
                        failed += num_rows
                        aitools.print_failed_reviews(current_offset)

                        # skip the batch if failed 3x
                        get_next_offset(num_rows)
                        num_rows = DEFAULT_NUM_ROWS


                    update_global_counters(completed, failed, conn, print_status=True)
                    completed = 0
                    failed = 0
                    try_count = 1
                
            except Exception as e:
                aitools.print_failed_reviews(current_offset, error=e)
                aitools.print_thread_count(end='\n')
                break


def threaded():
    """
    Executes sentiment analysis across multiple threads.
    """
    # Define the number of threads to use
    # max workers defaults to processors * 5 - test no max_workers value
    num_threads = os.cpu_count() * 4

    # Use ThreadPoolExecutor to execute analyse_sentiment in parallel
    with futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda _: analyse_sentiment(), range(num_threads))

if __name__ == '__main__':
    threaded()