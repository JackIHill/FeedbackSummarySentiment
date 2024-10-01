import pandas as pd
import sqlalchemy as sa

def insert_reviews(temptbl, unprocessed_sentiment=True): 
    join = ''
    if unprocessed_sentiment:
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
            WHERE r.ReviewText IS NOT NULL
            """
    return query

def get_remaining_sentiment_rows(offset, num_rows, conn):
    query = f"""
            SELECT ReviewID, ReviewText
            FROM #review_no_sentiment
            WHERE rn = 1
            ORDER BY ReviewID DESC
            OFFSET {offset} ROWS
            FETCH NEXT {num_rows} ROWS ONLY
            """
    
    rows = pd.read_sql(sa.text(query), conn)
    return rows

def get_count_remaining(conn):
    query = f"""
            SELECT count(ReviewID)
            FROM Review r
            INNER JOIN Sentiment s
                ON s.SentimentID = r.ReviewSentimentID
                AND s.SentimentText = 'Unknown - Unprocessed' 
            WHERE ReviewText IS NOT NULL
            """
    
    count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())

    return count

def sentiment_prompt(json):
    prompt = f"""
            The following JSON contains restaurant reviews (ReviewText).
            Each review is a separate entry. Stopwords have been filtered out of the ReviewText
            Rate the sentiment of each review from 0 to 10
            0 indicates highly negative sentiment, 5 is neutral sentiment, and 10 is highly positive.
            If sentiment unknown, return -1

            Return the ReviewID for the corresponding ReviewText and its sentiment rating.
            Ensure the returned ReviewID is in the input list of ReviewID and all input ReviewIDs are returned.

            Here are the reviews: \n\n{json}
            """
    return prompt


def update_review_tbl_query():
    query = """
            WITH cte as (
                SELECT
                r.ReviewID,
                r.ReviewText, 
                CASE
                    WHEN t.Sentiment = -1.2 THEN '-' ELSE CAST(t.Sentiment AS NVARCHAR(4))
                END AS Sentiment
                FROM #temp t
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

def count_completed(conn):
    query = """
                WITH cte as (
                    SELECT
                    r.ReviewID,
                    r.ReviewText
                    FROM #temp t
                    INNER JOIN Review r ON r.ReviewID = t.ReviewID
                    )
                SELECT count(*)
                FROM Review r
                INNER JOIN cte on cte.ReviewText = r.ReviewText
                """
    
    count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())

    return count

def delete_completed_from_temp(delete_list):
    query = f"""
            DELETE FROM #review_no_sentiment
            WHERE ReviewText in ({delete_list})
            """
    return query


JSON_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "review_sentiment",
        "schema": {
            "type": "object",
                "properties": {
                    "review_sentiment": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "ReviewID": {
                                    "type": "number"
                                },
                                "Sentiment": {
                                    "type": "number"
                                }
                            },
                            "required": [
                                "ReviewID",
                                "Sentiment"
                                ],
                            "additionalProperties": False
                        
                        } 
                }
            },
            "required": ["review_sentiment"],
            "additionalProperties": False
        }  
        ,"strict": True
    }       
}
