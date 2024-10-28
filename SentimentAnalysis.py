import sqlalchemy as sa

 # for type hints
from sqlalchemy.engine.base import Connection, Engine
from openai import OpenAI
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

import logging
from pathlib import Path


# TODO: centralise all hardcoded values e.g. default num rows, min_date, reduce_factor, max_retries

stops = aitools.get_stops()

MIN_REVIEW_DATEID: Optional[int] = None

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

@dataclass
class Shared():
    offset: int = 0
    remaining: Optional[int] = None
    completed: int = 0
    failed: int = 0
    printed: bool = False

    count_lock = threading.Lock()
    update_lock = threading.Lock()
    offset_lock = threading.Lock()
    print_lock = threading.Lock()
    queue_lock = threading.Lock()

    id_queue = deque()


class AnalyseSentiment:
    DEFAULT_NUM_ROWS = 40
    DEFAULT_REVIEW_TEMP_NAME = 'review_no_sentiment'
    DEFAULT_NUM_WORKERS = os.cpu_count() * 5
    
    logger = aitools.create_logger()

    def __init__(
            self,
            num_rows: Optional[int] = None,
            review_temp_name: Optional[str] = None,
            workers: Optional[int] = None,
            print_thread_count: bool = True
            ):
        
        self.shared = Shared()
        self.try_count = 0
        self.completed = 0
        self.failed = 0

        self.num_rows = num_rows if num_rows is not None else self.DEFAULT_NUM_ROWS
        self.review_temp_name = review_temp_name if review_temp_name is not None else self.DEFAULT_REVIEW_TEMP_NAME
        self.workers = workers if workers is not None else self.DEFAULT_NUM_WORKERS

        self.print_thread_count = print_thread_count


    def update_global_counters(
        self,
        completed: int,
        failed: int,
        conn: Connection,
        print_status: bool = True):

        with self.shared.count_lock:
            self.shared.completed += completed
            self.shared.failed += failed
            self.shared.remaining = senttools.get_count_remaining(conn, MIN_REVIEW_DATEID)

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


    def fetch_reviews(self, engine: Engine, review_temp_name: str):
        with engine.begin() as conn:
            # Drop and recreate the temporary table
            with self.shared.print_lock:
                if not self.shared.printed:
                    print(f'\nFetching Reviews...\r')
                    self.shared.printed = True
                    aitools.move_cursor_up()
            
            with self.shared.update_lock:
                with_retry(conn, aitools.drop_tbl_query(review_temp_name))
                with_retry(conn, senttools.insert_reviews(review_temp_name, MIN_REVIEW_DATEID))

            print('Reviews fetched for all threads. Processing...', end='\r')


    def analyse_sentiment(
            self,
            client: OpenAI,
            engine: Engine,
            review_temp_name: str):
        
        if self.print_thread_count:
            aitools.print_thread_count()

        while True:
            with engine.begin() as conn:
                if self.try_count == 0:
                    self.completed = 0
                    self.failed = 0
                    self.num_rows = self.DEFAULT_NUM_ROWS

                    current_offset = self.get_next_offset()

                self.logger.info(f'anlaysing rows {current_offset} to {current_offset + self.num_rows}')
              
                try:
                    if self.shared.remaining is None:
                        with self.shared.update_lock:
                            if self.shared.remaining is None:
                                self.shared.remaining = senttools.get_count_remaining(
                                    conn,
                                    MIN_REVIEW_DATEID
                                    )

                    # Fetch rows for sentiment analysis
                    reviews = senttools.get_remaining_sentiment_rows(review_temp_name, current_offset, self.num_rows, conn)

                    if reviews.empty:
                        print(f"No more reviews to process at offset {current_offset:<50}")
                        break

                    # raw_reviews = reviews['ReviewText']
                    
                # TODO: split below review processing into own func

                    # remove stop words and non-alpha characters
                    reviews.ReviewText = reviews.ReviewText.apply(
                                lambda x: ' '.join(
                                    [word for word in x.split() if word not in (stops)]
                                    )
                    ).str.replace('[^a-zA-Z ]', '', regex=True).str.strip()

                    review_json = reviews.to_json(orient='records')

                # TODO: split below api calling to own func
                    prompt = senttools.sentiment_prompt(review_json, self.num_rows)

                    output_table = aitools.process_completion(client, prompt, senttools.JSON_FORMAT)

                    # Sentiment 10 -> 1
                    # Sentiment 5 -> 0
                    # Sentiment 0 -> -1
                    # Previously Unknown Sentiment of -1 now = -1.2. Set to '-' in SQL.
                    output_table["Sentiment"] = (output_table["Sentiment"] - 5) / 5
                    
                    inputIDs = reviews['ReviewID'].to_list()
                    outputIDs = output_table['ReviewID'].to_list()


                    # ensure that no two threaads have updated the same id more than once.
                    # this should hopefully never happen due to offset increment. 
                    if self.try_count == 0:
                        with self.shared.queue_lock:
                            duplicate_found = any(i in list(self.shared.id_queue) for i in inputIDs)
                            if duplicate_found:
                                # TODO: skip batch or retry instead...
                                print('Duplicate IDs found!')
                                quit()
                            
                            # Add IDs to the queue

                        for i in inputIDs:
                            self.shared.id_queue.append(i)

                    output_sentiment = output_table['Sentiment'].to_list()
                    invalid_output_sentiment = [
                        x for x in set(output_sentiment) if not -1.2 <= x <= 1.0 or (x * 10) % 2 != 0
                        ]

                    sentiment_temp_name = '#temp'
                    reduce_factor = 0.2
                    if self.try_count != 3:
                        # TODO: consider repeatedly inserting into pd df,
                        # then doing a batch update when that df has e.g. 1000 rows
                        with self.shared.update_lock:
                            if inputIDs == outputIDs and not invalid_output_sentiment:
                                conn.execute(sa.text(aitools.drop_tbl_query(sentiment_temp_name)))
                                aitools.table_to_sqltbl(
                                    output_table,
                                    sentiment_temp_name,
                                    'ReviewID',
                                    conn)
                                
                                conn.execute(sa.text(senttools.update_review_tbl_query(sentiment_temp_name)))
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
                        
                        # TODO: add logging
                        aitools.print_failed_review_err(current_offset)

                        # skip the batch if failed 3x
                        current_offset = self.get_next_offset(self.num_rows)

                    self.update_global_counters(self.completed, self.failed, conn, print_status=True)
                    self.try_count = 0

                except Exception as e:
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
    # os.cpu_count() * 5
    analyser = AnalyseSentiment(print_thread_count=False)
    analyser.threaded()
