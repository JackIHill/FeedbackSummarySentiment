import sqlalchemy as sa

 # for type hints
from sqlalchemy.engine.base import Connection, Engine
from openai import OpenAI, RateLimitError

from typing import Optional
import pyodbc

from credentials.SQL_Credentials import username, password, server, database, driver
from credentials.OpenAI_API_Key import API_KEY

import tools.sentimenttools as senttools
import tools.aitools as aitools

import threading
from collections import deque
from concurrent import futures

from dataclasses import dataclass
import os
import time


# TODO: parameterise all hardcoded values e.g. default num rows, min_date, reduce_factor, max_retries
# TODO: docstrings
# TODO: comments throughout
# TODO: apply with_retry decorator to update functions?

stops = aitools.get_stops()

MIN_REVIEW_DATEID: Optional[int] = 20230201

# TODO: make decorator
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

@dataclass
class Shared():
    offset: int = 0
    remaining: Optional[int] = None
    completed: int = 0
    failed: int = 0
    printed: bool = False
    insert_query: str = None

    count_lock = threading.Lock()
    update_lock = threading.Lock()
    offset_lock = threading.Lock()
    print_lock = threading.Lock()
    queue_lock = threading.Lock()

    id_queue = deque()

    total_to_process = 0

class AnalyseSentiment:
    DEFAULT_NUM_ROWS = 40
    DEFAULT_REVIEW_TEMP_NAME = 'StagingReviews_SentimentProcessing'
    DEFAULT_NUM_WORKERS = os.cpu_count() * 3
    
    logger = aitools.create_logger()

    def __init__(
            self,
            num_rows: Optional[int] = None,
            phrase_list: list = None,
            operator_list: list = None,
            review_temp_name: Optional[str] = None,
            workers: Optional[int] = None,
            print_thread_count: bool = True,
            ):
        
        self.shared = Shared()
        self.try_count = 0
        self.completed = 0
        self.failed = 0

        self.num_rows = self.DEFAULT_NUM_ROWS
        if num_rows is not None:
            self.num_rows = num_rows
            self.DEFAULT_NUM_ROWS = num_rows
        
        self.phrase_list = [phrase.title().strip() for phrase in phrase_list] if phrase_list is not None else None
        self.operator_list = operator_list

        self.review_temp_name = review_temp_name if review_temp_name is not None else self.DEFAULT_REVIEW_TEMP_NAME
        self.workers = workers if workers is not None else self.DEFAULT_NUM_WORKERS

        self.print_thread_count = print_thread_count


    def update_global_counters(
        self,
        completed: int,
        failed: int,
        conn: Connection,
        print_status: bool = True):

        remaining_count = senttools.get_count_remaining(conn, self.shared.insert_query)

        with self.shared.count_lock:
            self.shared.completed += completed
            self.shared.failed += failed
            self.shared.remaining = remaining_count

        if print_status:
            aitools.print_result(
                self.DEFAULT_NUM_ROWS,
                self.shared.completed,
                self.shared.remaining,
                self.shared.failed)


    # should probably use queue for this and fill with values from 1 to num_rows?
    def get_next_offset(self, increment_by: Optional[int] = None) -> int:
        if not increment_by:
            increment_by = self.DEFAULT_NUM_ROWS

        with self.shared.offset_lock:
            current_offset = self.shared.offset
            self.shared.offset += increment_by

            return current_offset


    def fetch_reviews(
            self,
            engine: Engine,
            review_temp_name: str
            ):
    
        with engine.begin() as conn:
            # Drop and recreate the temporary table:
            with self.shared.print_lock:
                if not self.shared.printed:
                    print(f'\nFetching Reviews...\r')
                    self.shared.printed = True
                    
                    # ensure following print overwrites 'Fetching Reviews...' 
                    aitools.move_cursor_up()
            
            self.shared.insert_query = senttools.insert_reviews(
                review_temp_name,
                MIN_REVIEW_DATEID,
                self.operator_list,
                self.phrase_list
                )
            
            with self.shared.update_lock:
                self.shared.total_to_process = senttools.get_count_remaining(conn, self.shared.insert_query)

                with_retry(conn, aitools.drop_tbl_query(review_temp_name))
                with_retry(conn, self.shared.insert_query)

            print('Reviews fetched for all threads. Processing...', end='\r')
        

    def analyse_sentiment(
            self,
            client: OpenAI,
            engine: Engine,
            review_temp_name: str):
        
        if self.print_thread_count:
            aitools.print_thread_count()
        
        incremented_after_failure = False
        while True:
            with engine.begin() as conn:
                if self.try_count == 0:
                    self.completed = 0
                    self.failed = 0
                    self.num_rows = self.DEFAULT_NUM_ROWS

                    if not incremented_after_failure:
                        current_offset = self.get_next_offset()
                    incremented_after_failure = False

                try:
                    with self.shared.update_lock:
                        remaining_count = senttools.get_count_remaining(conn, self.shared.insert_query)

                    if self.shared.remaining is None:
                        with self.shared.count_lock:
                            if self.shared.remaining is None:
                                self.shared.remaining = remaining_count

                    if current_offset >= self.shared.total_to_process:
                        break

                    self.logger.info(f'analysing rows {current_offset} to {current_offset + self.num_rows}')

                    # Fetch rows for sentiment analysis
                    reviews = senttools.fetch_next_batch(review_temp_name, current_offset, self.num_rows, conn)

                    if reviews.empty:
                        print(f"No more reviews to process at offset {current_offset:<50}")
                        break
                    
                # TODO: split below review processing into own func

                    # remove stop words and non-alpha characters
                    reviews.ReviewText = reviews.ReviewText.apply(
                                lambda x: ' '.join(
                                    [word for word in x.split() if word not in (stops)]
                                    )
                    ).str.replace('[^a-zA-Z ]', '', regex=True).str.strip()

                    # TODO: append json to dated(?) log files
                    review_json = reviews.to_json(orient='records')

                # TODO: split below api calling to own func
                    # make a general 'prompt' function with phrase bool arg?
                    if self.phrase_list:
                        prompt = senttools.phrase_prompt(review_json, self.phrase_list, self.num_rows)
                        json_format = senttools.JSON_FORMAT_Phrase
                    else:
                        prompt = senttools.sentiment_prompt(review_json, self.num_rows)
                        json_format = senttools.JSON_FORMAT

                    output_table = aitools.process_completion(client, prompt, json_format)
               
                    # TODO: Output table sometimes returns None for phrase completions.
                    # figure out why, or retry that batch if error.


                    # Sentiment 10 -> 1
                    # Sentiment 5 -> 0
                    # Sentiment 0 -> -1
                    # Previously Unknown Sentiment of -1 now = -1.2. Set to '-' in SQL.
                    output_table["Sentiment"] = (output_table["Sentiment"] - 5) / 5
                    
                    inputIDs = reviews['ReviewID'].to_list()
                    outputIDs = output_table['ReviewID'].to_list()
        
                    # ensure that no two threaads have updated the same id more than once.
                    # this should hopefully never happen due to offset increment (but sometimes does, why, help). 
                    duplicate_found = None
                    if self.try_count == 0:
                        with self.shared.queue_lock:
                            duplicate_found = any(i in list(self.shared.id_queue) for i in inputIDs)
                            # if duplicate_found:
                            #     # TODO: log
                            #     print('Duplicate IDs found!')
                            #     print()
                            
                            # Add IDs to the queue
                            for i in inputIDs:
                                self.shared.id_queue.append(i)

                    output_sentiment = output_table['Sentiment'].to_list()

                    # -1.2, -1 ... 0.8, 1
                    valid_output = [
                        i / 10.0 for i in range(
                            int(-1.2 * 10),
                            (int(1.0 * 10)) + 1,
                            2)
                        ]
                    
                    invalid_output_sentiment = [
                        x for x in set(output_sentiment) if x not in valid_output
                        ]

                    sentiment_temp_name = '#temp'
                    sentiment_temp_name2 = '#temp2'
                    reduce_factor = 0.2
                    
                    if self.try_count != 3:
                        # TODO: consider repeatedly inserting into pd df,
                        # then doing a batch update when that df has e.g. 1000 rows
                        if inputIDs == outputIDs and not invalid_output_sentiment and not duplicate_found:
                            with self.shared.update_lock:
                                conn.execute(sa.text(aitools.drop_tbl_query(sentiment_temp_name)))
                                conn.execute(sa.text(aitools.drop_tbl_query(sentiment_temp_name2)))

                                aitools.table_to_sqltbl(
                                    output_table,
                                    sentiment_temp_name,
                                    conn,
                                    'ReviewID',)
                                

                                if not self.phrase_list:
                                    conn.execute(sa.text(senttools.update_review_tbl_query(sentiment_temp_name)))
                                else:
                                    conn.execute(sa.text(senttools.update_phrase_tbl_query(self.phrase_list)))

                                completed_rows = senttools.count_completed(sentiment_temp_name, conn)
                                
                                self.completed += completed_rows
                            
                        else:
                            self.try_count += 1
                            self.num_rows = max(1, int(self.num_rows * (1 - reduce_factor)))

                            # remove bad batch from ID queue
                            # IDs in the reduced batch will be appended next loop.               
                            for i in inputIDs:
                                with self.shared.queue_lock:
                                    if i in list(self.shared.id_queue):
                                        self.shared.id_queue.remove(i)

                            continue

                    else:                        
                        self.failed += self.num_rows
                        new_line = '\n'
                        self.logger.info(f"""FAILED: 
                                        REVIEWIDS: {str(f'{new_line}'.join(str(i) for i in inputIDs))}
                                        OUTPUT: {output_table}
                                        INVALID_SENTIMENT: {invalid_output_sentiment}
                                        INPUT = OUTPUT: {inputIDs == outputIDs}
                                        """)                                    
                        aitools.print_failed_review_err(current_offset)
                        
                        # skip the batch if failed 3x
                        # TODO: this might be causing the loop bug??
                        current_offset = self.get_next_offset(self.num_rows)
                        incremented_after_failure = True
                        
                    self.logger.info(f'Completed at offset {str(current_offset)}')
                    self.update_global_counters(self.completed, self.failed, conn, print_status=True)
                    
                    # print('YEP YEPO YEP')
                    
                    self.try_count = 0
                   
                    # TODO: log try count to try to figure out apparent loop bug? Only happens intermittently so possible
                    # request / api issue?

                except Exception as e:
                    print('nope')
                    aitools.print_failed_review_err(current_offset, error=e)

                    if self.print_thread_count:
                        aitools.print_thread_count(end='\n')
                    break


    def threaded(self):
        """
        Executes sentiment analysis across multiple threads.
        """        
        num_workers = self.workers

        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            actual_num_workers = executor._max_workers

            client, engine = aitools.establish_connection(
                API_KEY,
                username,
                password,
                server,
                database,
                driver,
                actual_num_workers
                )
            
            self.fetch_reviews(engine, self.review_temp_name)

            executor.map(
                lambda _: self.analyse_sentiment(client, engine, self.review_temp_name),
                        range(actual_num_workers)
                )


if __name__ == '__main__':
    # run analyser with no params for general sentiment scoring
    phrase_list=['Drinks']
    operator_list = ['ROSAS THAI',
                    'PHO',
                    'MOWGLI',
                    'GIGGLING SQUID',
                    'BANANA TREE',
                    'NANDOS',
                    'ZAAP THAI',
                    'DISHOOM',
                    'COTE',
                    'WAGAMAMA',
                    'THE IVY'
                    ]
    analyser = AnalyseSentiment(operator_list=operator_list, phrase_list=phrase_list)
    # analyser = AnalyseSentiment()
    analyser.threaded()
  