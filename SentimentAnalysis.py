import sqlalchemy as sa

 # for type hints
from sqlalchemy.engine.base import Connection, Engine
from openai import OpenAI, RateLimitError

import traceback

from typing import Optional
import pyodbc

from credentials.SQL_Credentials import username, password, server, database, driver
from credentials.OpenAI_API_Key import API_KEY

import tools.sentimenttools as senttools
import tools.aitools as aitools

import threading
from collections import deque, defaultdict
from concurrent import futures

from dataclasses import dataclass
import os
import time

import pandas as pd


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
    remaining_unique_texts: Optional[int] = None
    completed: int = 0
    failed: int = 0
    printed: bool = False
    insert_query: str = None

    count_lock = threading.Lock()
    update_lock = threading.Lock()
    print_lock = threading.Lock()

    review_queue = deque()
    review_map = defaultdict(list)
    processed_reviews = set()

    total_to_process = 0

class AnalyseSentiment:
    DEFAULT_NUM_ROWS = 50
    DEFAULT_REVIEW_TEMP_NAME = 'StagingReviews_SentimentProcessing'
    DEFAULT_NUM_WORKERS = os.cpu_count() * 5
    
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

        remaining_count = len(self.shared.review_queue)

        with self.shared.count_lock:
            self.shared.completed += completed
            self.shared.failed += failed
            self.shared.remaining_unique_texts = remaining_count

        if print_status:
            aitools.print_result(
                self.DEFAULT_NUM_ROWS,
                self.shared.completed,
                self.shared.remaining_unique_texts,
                self.shared.failed)


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
            

            conn.execute(sa.text(f""";with cte as (
                                    select distinct ReviewText, psentr.PhraseSentimentFlagID
                                    from Phrase_SentimentReview psentr
                                    inner join review on review.reviewid = psentr.reviewid
                                )
                                insert into Phrase_SentimentReview (ReviewID, PhraseSentimentFlagID)
                                select distinct review.ReviewID, cte.PhraseSentimentFlagID
                                from review
                                inner join cte on cte.ReviewText = review.ReviewText
                                where not exists (
                                    select * from Phrase_SentimentReview psentr
                                    where psentr.ReviewID = review.ReviewID
                                    and psentr.PhraseSentimentFlagID = cte.PhraseSentimentFlagID
                                )
                                and cte.ReviewText = 'Staff here are lovely'"""
                                ))
            conn.commit()
        
        with engine.begin() as conn:
            self.shared.insert_query = senttools.insert_reviews(
                review_temp_name,
                MIN_REVIEW_DATEID,
                self.operator_list,
                self.phrase_list
                )
            
            rows = pd.read_sql(self.shared.insert_query, conn)  

            rows['ReviewText'] = rows['ReviewText'].str.lower().apply(
                                                    lambda x: ' '.join(
                                                        [word for word in x.split() if word not in (stops)]
                                                        )
                                                    ).str.replace('[^a-zA-Z ]', '', regex=True).str.strip()

            for _, row in rows.iterrows():
                review_id, review_text = row['ReviewID'], row['ReviewText']
                
                self.shared.review_queue.append((review_id, review_text))
                self.shared.review_map[review_text].append(review_id)

        
            self.shared.review_queue = deque((ids[0], text) for text, ids in self.shared.review_map.items())

            self.shared.total_to_process = len(self.shared.review_queue)

            print('Reviews fetched for all threads. Processing...', end='\r')
        

    def analyse_sentiment(
            self,
            client: OpenAI,
            engine: Engine,
            review_temp_name: str):
        
        if self.print_thread_count:
            aitools.print_thread_count()
        
        batch = []
        while self.shared.review_queue:
            with engine.begin() as conn:
                if self.try_count == 0:
                    self.completed = 0
                    self.failed = 0
                    self.num_rows = self.DEFAULT_NUM_ROWS
                    remaining_count = len(self.shared.review_queue)

                try:
                    if self.shared.remaining_unique_texts is None:
                        with self.shared.count_lock:
                            if self.shared.remaining_unique_texts is None:
                                self.shared.remaining_unique_texts = remaining_count


                    # Fetch rows
                    if self.try_count == 0:
                        batch = []
                        for _ in range(min(self.num_rows, len(self.shared.review_queue))):
                            review_id, review_text = self.shared.review_queue.popleft()
                            batch.append((review_id, review_text, self.shared.review_map[review_text]))

                    else:                        
                        self.shared.review_queue.extend(batch[self.num_rows:])
                        batch = batch[:self.num_rows]


                    # TODO: append json to dated(?) log files
                    batch_json = [
                        {'ReviewID': rev_id, 'ReviewText': rev_text} for rev_id, rev_text, _ in batch
                    ]


                # TODO: split below api calling to own func
                    # make a general 'prompt' function with phrase bool arg?
                    if self.phrase_list:
                        prompt = senttools.phrase_prompt(batch_json, self.phrase_list, self.num_rows)
                        json_format = senttools.JSON_FORMAT_Phrase
                    else:
                        prompt = senttools.sentiment_prompt(batch_json, self.num_rows)
                        json_format = senttools.JSON_FORMAT

                    processed_table = aitools.process_completion(client, prompt, json_format)
                    batch_df = pd.DataFrame(batch, columns=['ReviewID', 'ReviewText', 'MatchedTextIDs'])

                    # print("batch_df columns:", batch_df.columns)
                    # print("processed_table columns:", processed_table.columns)
                    
                    merge_batch_output_df = pd.merge(batch_df, processed_table, on='ReviewID', how='left')
                    exploded_merge_df = merge_batch_output_df.explode('MatchedTextIDs').reset_index()

                    final_output = exploded_merge_df[['MatchedTextIDs', 'PhraseFlag', 'Sentiment']]
                    final_output = final_output.rename(columns={'MatchedTextIDs': 'ReviewID'})


                    # TODO: Output table sometimes returns None for phrase completions.
                    # figure out why, or retry that batch if error.

                    # Sentiment 10 -> 1
                    # Sentiment 5 -> 0
                    # Sentiment 0 -> -1
                    # Previously Unknown Sentiment of -1 now = -1.2. Set to '-' in SQL.
                    final_output["Sentiment"] = (final_output["Sentiment"] - 5) / 5
                    
                    inputIDs = [rev_id for rev_id, _, _ in batch]
                    outputIDs = processed_table['ReviewID'].to_list()

                    output_sentiment = final_output['Sentiment'].to_list()

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
                        if inputIDs == outputIDs and not invalid_output_sentiment:
                            with self.shared.update_lock:
                                conn.execute(sa.text(aitools.drop_tbl_query(sentiment_temp_name)))
                                conn.execute(sa.text(aitools.drop_tbl_query(sentiment_temp_name2)))

                                aitools.table_to_sqltbl(
                                    final_output,
                                    sentiment_temp_name,
                                    conn,
                                    'ReviewID',)
                                

                                if not self.phrase_list:
                                    conn.execute(sa.text(senttools.update_review_tbl_query(sentiment_temp_name)))
                                else:
                                    conn.execute(sa.text(senttools.update_phrase_tbl_query(self.phrase_list)))

                                inserted_rows = len(final_output.index)
                                completed = len(batch)
                                self.completed += completed
                            
                        else:
                            self.try_count += 1
                            self.num_rows = max(1, int(self.num_rows * (1 - reduce_factor)))

                            continue

                    else:                        
                        self.failed += self.num_rows
                        new_line = '\n'
                        self.logger.info(f"""FAILED: 
                                        REVIEWIDS: {str(f'{new_line}'.join(str(i) for i in inputIDs))}
                                        OUTPUT: {final_output}
                                        INVALID_SENTIMENT: {invalid_output_sentiment}
                                        INPUT = OUTPUT: {inputIDs == outputIDs}
                                        """)      
                        
                        # batch failed, dump back into queue for second chance later.

                        #below 2 should be identical
                        # self.shared.review_queue.extend(batch[:self.num_rows])
                        self.shared.review_queue.extend(batch)
                        
                        self.update_global_counters(self.completed, self.failed, conn, print_status=True)
                    
                    self.try_count = 0

                except Exception as e:
                    print("Traceback:\n", traceback.format_exc())

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
    phrase_list=['Atmosphere']
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
  