import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.base import Connection

from typing import Optional


def min_date_query(min_review_dateid: Optional[int]) -> str:
    where = ''
    if min_review_dateid:
        where = f"""AND r2.Review_DateID >= {min_review_dateid}"""
    return where 


def operator_join_where(operator_list: Optional[list]) -> tuple[str, str]:
    op_join = where_op = ''
    if operator_list:
        operators = str(set(operator_list))[1:-1]
        op_join = """INNER JOIN Venue v ON r2.VenueID = v.VenueID
                    INNER JOIN vw_VenueExport vw ON vw.OperatorVenueCode = v.OperatorVenueCode"""
        where_op = f"""AND vw.OperatorName in ({operators})"""
     
    return op_join, where_op


def insert_reviews(
        temptbl: str,
        min_review_dateid: Optional[int] = None,
        operator_list: Optional[list] = None,
        phrase_list: list = None) -> str: 
    
    # TODO: needs a cleanup

    where = ''
    join = ''
    where_date = min_date_query(min_review_dateid)
    op_join, where_op = operator_join_where(operator_list)

    if phrase_list:
        phrase_list = ' or '.join(phrase_list)

        where = f"""AND ReviewID NOT IN (
                        SELECT r2.ReviewID FROM Phrase_SentimentReview psr
                        INNER JOIN review r2 on psr.reviewid = r2.reviewid
                        INNER JOIN Phrase_SentimentFlag psf ON psf.PhraseSentimentFlagID = psr.PhraseSentimentFlagID 
                        AND psf.Phrase = '{phrase_list}'
                )"""
    else:
        join = """
            INNER JOIN Sentiment s
                ON s.SentimentID = r2.ReviewSentimentID
                AND s.SentimentText = 'Unknown - Unprocessed' 
            """

    query = f"""
        SELECT ReviewID, ReviewText FROM (
            SELECT r.ReviewID, r.ReviewText, max(Review_DateID) over(partition by ReviewText order by reviewid) max_review_date
            FROM Review r
            WHERE r.ReviewText IN (
                SELECT DISTINCT r2.ReviewText
                FROM Review r2
                {join}
                {op_join}
                WHERE 1=1 
                {where}
                {where_op}
                AND r2.ReviewText IS NOT NULL
            )
        ) x
        where x.max_review_date >= {min_review_dateid}

        """
    return query


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
            1. Assert that every 'ReviewID' from the input is in the output, and every 'ReviewID' from the output is in the input.
            2. No additional or missing 'ReviewID's are allowed.
            3. Before returning the output, verify that the 'ReviewID' list is identical to the input list. If any 'ReviewID' is extra or missing, fix it before returning.
            4. No 'ReviewID' should be skipped, even if the sentiment is unknown (use '-1' if unsure).
            5. The output must be a JSON list where each item contains a 'ReviewID' and its corresponding 'Sentiment'.
            6. There should be exactly {input_length} 'ReviewID's in the final output, discard and regenerate if there are not.    
                                                                
            Here are the reviews: \n\n{json}
            """
    return prompt


def phrase_prompt(json, phrase_list: list, input_length: int) -> str:
    phrase_list = (' or '.join(phrase_list)).lower()

    prompt = f"""
            The input is a JSON list of restaurant/pub reviews, each with a 'ReviewID' and 'ReviewText'.

            1. Identify phrase mentions: For each review, check if the review discusses {phrase_list} or any related terms/synonyms.
               If discussed, set PhraseFlag = 1; otherwise set PhraseFlag = 0.

                Example Usage:
                If identifying "Price or Value" then you'd check for mentions of terms such as "price", "value", "cost", or "worth".
                If identifying "Staff Members" then you'd check for mentions of terms such as "Staff", "Manager", "Waiter" or "Bartender".
                
                If PhraseFlag = 1, remember the synonyms you found when flagging Phrase presence
                to better inform the following stage.

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
            4. No 'ReviewID' should be skipped, even if the sentiment is unknown (use '-1' if unsure).

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
            FROM cte
            INNER JOIN Review r on r.ReviewID = cte.ReviewID
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
            INNER JOIN Review r on r.ReviewID = cte.ReviewID
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
