import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.base import Connection

from typing import Optional


def min_date_query(min_review_dateid: Optional[int]) -> str:
    where = ''
    if min_review_dateid:
        where = f"""AND Review_DateID >= {min_review_dateid}"""
    return where 


def operator_join_where(operator_list: Optional[list]) -> tuple[str, str]:
    op_join = where_op = ''
    if operator_list:
        operators = str(set(operator_list))[1:-1]
        op_join = """INNER JOIN Venue v ON r.VenueID = v.VenueID
                    INNER JOIN vw_VenueExport vw ON vw.OperatorVenueCode = v.OperatorVenueCode"""
        where_op = f"""AND vw.OperatorName in ({operators})"""
     
    return op_join, where_op


def insert_reviews(
        temptbl: str,
        min_review_dateid: Optional[int] = None,
        operator_list: Optional[list] = None,
        phrase_list: list = None) -> str: 
    
    where = ''
    join = ''
    where_date = min_date_query(min_review_dateid)
    op_join, where_op = operator_join_where(operator_list)

    if phrase_list:
        phrase_list = ' or '.join(phrase_list)

        where = f"""AND ReviewID NOT IN (
                        SELECT ReviewID FROM Phrase_SentimentReview psr
                        INNER JOIN Phrase_SentimentFlag psf ON psf.PhraseSentimentFlagID = psr.PhraseSentimentFlagID 
                        AND psf.Phrase = '{phrase_list}'
                )"""
    else:
        join = """
            INNER JOIN Sentiment s
                ON s.SentimentID = r.ReviewSentimentID
                AND s.SentimentText = 'Unknown - Unprocessed' 
            """

    query = f"""
            SELECT r.ReviewID, r.ReviewText
                    ,ROW_NUMBER() OVER (PARTITION BY r.ReviewText ORDER BY r.ReviewID DESC) as rn
            INTO {temptbl}
            FROM Review r
            {join}
            {op_join}
            WHERE 1=1 
            {where}
            {where_op}
            {where_date}
            AND r.ReviewText IS NOT NULL
            """
    return query


def fetch_next_batch(
        from_tbl: str,
        offset: int,
        num_rows: int,
        conn: Connection) -> pd.DataFrame:
    
    query = f"""
            SELECT ReviewID, ReviewText
            FROM {from_tbl} with (nolock)
            WHERE rn = 1
            ORDER BY ReviewID DESC
            OFFSET {offset} ROWS
            FETCH NEXT {num_rows} ROWS ONLY
            """
    rows = pd.read_sql(sa.text(query), conn)
    return rows 


def get_count_remaining(
    conn: Connection,
    min_review_dateid: Optional[int] = None,
    operator_list: Optional[list] = None,
    phrase_list: list = None) -> int:

    # when move to ORM, add **kwargs for where clause.
    where = ''
    join = ''
    where_date = min_date_query(min_review_dateid)
    op_join, where_op = operator_join_where(operator_list)

    if phrase_list:
        phrase_list = ' or '.join(phrase_list)

        where = f"""AND ReviewID NOT IN (
                        SELECT ReviewID FROM Phrase_SentimentReview psr
                        INNER JOIN Phrase_SentimentFlag psf ON psf.PhraseSentimentFlagID = psr.PhraseSentimentFlagID 
                        AND psf.Phrase = '{phrase_list}'
                )"""
    else:
        join = """INNER JOIN Sentiment s
                    ON s.SentimentID = r.ReviewSentimentID
                    AND s.SentimentText = 'Unknown - Unprocessed' """

    query = f"""
            SELECT count(ReviewID)
            FROM Review r
            {join}
            {op_join}
            WHERE 1=1 
            {where}
            {where_op}
            {where_date}
            AND r.ReviewText IS NOT NULL
            """
    
    count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())
    return count


def sentiment_prompt(json: str, input_length: int) -> str:
    prompt = f"""
            Analyze restaurant reviews and return a sentiment score for each review.
            The input is a JSON list of reviews, each with a 'ReviewID' and 'ReviewText'.
            The sentiment should be rated from 0 to 10:
            - '0' is highly negative.
            - '5' is neutral.
            - '10' is highly positive.
            - -1 is unknown/unclear sentiment.

            Requirements:
            1. Each review must have a 'ReviewID' in the output, exactly as it appears in the input.
            2. No 'ReviewID' should be skipped, even if the sentiment is unknown (use '-1' if unsure).
            3. The output must be a JSON list where each item contains a 'ReviewID' and its corresponding 'Sentiment'.
            4. There should be no more than {input_length} 'ReviewID's in the output.    
                                                                
            Here are the reviews: \n\n{json}
            """
    return prompt


def phrase_prompt(json, phrase_list: list, input_length: int) -> str:
    phrase_list = (' or '.join(phrase_list)).lower()

    prompt = f"""
            The input is a JSON list of reviews, each with a 'ReviewID' and 'ReviewText'.

            1. Identify phrase mentions: For each review, check if the review discusses {phrase_list} or any related terms/synonyms.
               If discussed, set PhraseFlag = 1; otherwise set PhraseFlag = 0.

                Example Usage:
                If identifying "Price or Value" then check for mentions of terms like "price" "value" "cost" or "worth".

            2. Analyse Sentiment: If and only if PhraseFlag = 1, evaluate the sentiment specifically towards {phrase_list}
               or any related terms/synonyms within the review, from 0 to 10:

            - '0' is negative sentiment (e.g. "bad {phrase_list}")
            - '5' is neutral sentiment (e.g. "{phrase_list} was/were fine")
            - '10' is positive sentiment (e.g. "{phrase_list} was/were excellent"),
            - -1 is unknown/unclear sentiment.

            Requirements:
            1. Each review must have a 'ReviewID' in the output, exactly as it appears in the input.
            2. The output must be a JSON list where each item contains a 'ReviewID', a 'PhraseFlag' and its corresponding 'Sentiment'.
            3. There should be no more than {input_length} 'ReviewID's in the output.    

            Here are the reviews: \n\n{json}
            """
    return prompt


def update_review_tbl_query(temp_name: str) -> str:
    query = f"""
            WITH cte as (
                SELECT
                r.ReviewID,
                r.ReviewText, 
                CASE
                    WHEN t.Sentiment = -1.2 THEN '-' ELSE CAST(t.Sentiment AS NVARCHAR(4))
                END AS Sentiment
                FROM {temp_name} t
                INNER JOIN Review r ON r.ReviewID = t.ReviewID
                )
            UPDATE Review
            set ReviewSentimentID = s.SentimentID
            FROM Review r
            INNER JOIN cte on cte.ReviewText = r.ReviewText
            INNER JOIN Sentiment s
                on (cte.Sentiment = s.SentimentRawScore and s.SentimentText = 'Unknown - Processed')
                    or (cte.Sentiment = s.SentimentRawScore and cte.Sentiment <> '-')
            """
    return query


def update_phrase_tbl_query(phrase_list: list) -> str:
    phrase_list = ' or '.join(phrase_list)

    query = f"""
            WITH cte AS (
                SELECT
                t.ReviewID,
                r.ReviewText,
                '{phrase_list}' Phrase,
                t.PhraseFlag,
                CASE
                    WHEN t.PhraseFlag = 0 or t.Sentiment = -1.2
                    THEN '-'
                    ELSE CAST(t.Sentiment AS NVARCHAR(4))

                END AS PhraseSentiment
                FROM #temp t
                INNER JOIN Review r ON r.ReviewID = t.ReviewID

            )
            SELECT r.ReviewID, cte.Phrase, cte.PhraseFlag, s.SentimentID
            INTO #temp2 
            FROM cte
            INNER JOIN Review r on r.ReviewText = cte.ReviewText
            INNER JOIN Sentiment s
                on (cte.PhraseSentiment = s.SentimentRawScore and s.SentimentText = 'Unknown - Processed')
                or (cte.PhraseSentiment = s.SentimentRawScore and cte.PhraseSentiment <> '-')

                
            INSERT INTO Phrase_SentimentFlag (Phrase, PhraseFlag, PhraseSentimentID)
            SELECT distinct t2.Phrase, t2.PhraseFlag, t2.SentimentID
            FROM #temp2 t2
            WHERE NOT EXISTS (
                SELECT * FROM Phrase_SentimentFlag psf
                WHERE psf.Phrase = t2.Phrase
                    and psf.PhraseFlag = t2.PhraseFlag
                    and psf.PhraseSentimentID = t2.SentimentID
                )   
            
            INSERT INTO Phrase_SentimentReview (ReviewID, PhraseSentimentFlagID)
            SELECT t2.ReviewID, psf.PhraseSentimentFlagID
            FROM #temp2 t2
            INNER JOIN Phrase_SentimentFlag psf
            ON psf.Phrase = t2.Phrase
            AND psf.PhraseFlag = t2.PhraseFlag
            AND psf.PhraseSentimentID = t2.SentimentID
            WHERE NOT EXISTS (
                SELECT * FROM Phrase_SentimentReview psr
                WHERE psr.ReviewID = t2.ReviewID AND
                psr.PhraseSentimentFlagID = psf.PhraseSentimentFlagID
                )   
    """
    return query


def count_completed(temp_name: str, conn: Connection) -> int:
    query = f"""
                WITH cte as (
                    SELECT
                    r.ReviewID,
                    r.ReviewText
                    FROM {temp_name} t
                    INNER JOIN Review r ON r.ReviewID = t.ReviewID
                    )
                SELECT count(*)
                FROM Review r
                INNER JOIN cte on cte.ReviewText = r.ReviewText
                """
    
    count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())

    return count


def delete_completed_from_temp(temp_name: str, delete_list: str) -> str:
    query = f"""
            DELETE FROM {temp_name}
            WHERE ReviewText in ({delete_list})
            """
    return query


JSON_FORMAT = {
    'type': 'json_schema',
    'json_schema': {
        'name': 'review_sentiment',
        'schema': {
            'type': 'object',
                'properties': {
                    'review_sentiment': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'ReviewID': {
                                    'type': 'number'
                                },
                                'Sentiment': {
                                    'type': 'number'
                                }
                            },
                            'required': [
                                'ReviewID',
                                'Sentiment'
                                ],
                            'additionalProperties': False
                        
                        } 
                }
            },
            'required': ['review_sentiment'],
            'additionalProperties': False
        }  
        ,'strict': True
    }       
}


JSON_FORMAT_Phrase = {
    'type': 'json_schema',
    'json_schema': {
        'name': 'review_sentiment',
        'schema': {
            'type': 'object',
                'properties': {
                    'review_sentiment': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'ReviewID': {
                                    'type': 'number'
                                },
                                'PhraseFlag': {
                                    'type': 'number'
                                },
                                 'Sentiment': {
                                    'type': 'number'
                                }
                            },
                            'required': [
                                'ReviewID',
                                'PhraseFlag',
                                'Sentiment'
                                ],
                            'additionalProperties': False
                        
                        } 
                }
            },
            'required': ['review_sentiment'],
            'additionalProperties': False
        }  
        ,'strict': True
    }       
}
