
import os
import pandas as pd
from nltk import download
from nltk.corpus import stopwords

import sqlalchemy as sa
from credentials.SQL_Credentials import username, password, server, database, driver

from openai import OpenAI
from credentials.OpenAI_API_Key import API_KEY

import tools.sentimenttools as senttools
import tools.aitools as aitools


client, engine = aitools.establish_connection(API_KEY, username, password, server, database, driver)

download('stopwords')

DEFAULT_NUM_ROWS = 40
MIN_SCRAPE_DATEID = 20230101

completed = 0
failed = 0
offset = 0
try_count = 1
num_rows = DEFAULT_NUM_ROWS
while True:
    # Grab the first num_rows distinct ReviewTexts that don't have sentiment scores.
    # This ensures identical ReviewTexts later get given the same sentiment score. 
    review_temp_name = '#review_no_sentiment'
    with engine.begin() as conn:
        conn.execute(sa.text(aitools.drop_tbl(review_temp_name)))
        conn.execute(sa.text(senttools.insert_reviews(review_temp_name)))

        reviews = senttools.get_remaining_sentiment_rows(review_temp_name, offset, num_rows, conn)
        remaining = senttools.get_count_remaining(conn)


    raw_reviews = reviews['ReviewText']
    if remaining == 0:
        print('Sentiment rating for all Reviews has been completed.')
        break

    __location__ = os.path.realpath(
                os.path.join(os.getcwd(), os.path.dirname(__file__)))

    allowed_stops_file = open(os.path.join(__location__, r'tools/AllowedStopWords.csv'))
    allowed_stops = pd.read_csv(allowed_stops_file)['AllowedWords'].tolist()

    stop = [s for s in stopwords.words('english') if s not in allowed_stops]

    # Reduces text length by approx 20% = 20% less token usage for minimal cost in accuracy.
    reviews.ReviewText = reviews.ReviewText.apply(
                                lambda x: ' '.join([word for word in x.split() if word not in (stop)])
                                )

    # retain only alpha chars
    reviews.ReviewText = reviews.ReviewText.str.replace('[^a-zA-Z ]', '', regex=True).str.strip()

    # get first 20 words of each review to limit token usage.
    # Generally enough words to get a good read on sentiment
    reviews.ReviewText = reviews.ReviewText.apply(lambda x: ' '.join(x.split()[:20]))

    # Get average length of reviewtext after sanitisation
    # print(reviews.ReviewText.apply(len).mean())

    review_json = reviews.to_json(orient='records')

    prompt = senttools.sentiment_prompt(review_json)
    output_table = aitools.process_completion(client, prompt, senttools.JSON_FORMAT)

    # Sentiment 10 -> 1
    # Sentiment 5 -> 0
    # Sentiment 0 -> -1
    # Previously Unknown Sentiment of -1 now = -1.2. Set to '-' in SQL.
    output_table["Sentiment"] = (output_table["Sentiment"] - 5) / 5

            
    # check valid sentiment, no odd values.

    inputIDs = reviews['ReviewID'].to_list()
    outputIDs = output_table['ReviewID'].to_list()

    output_sentiment = output_table['Sentiment'].to_list()
    invalid_output_sentiment = [
        x for x in set(output_sentiment) if not -1.2 <= x <= 1.0 or (x * 10) % 2 != 0
        ]

    sentiment_temp_name = '#temp'
    if try_count != 3:
        if inputIDs == outputIDs and not invalid_output_sentiment:
            with engine.begin() as conn:
                conn.execute(sa.text(aitools.drop_tbl(sentiment_temp_name)))
                aitools.table_to_sqltbl(output_table, sentiment_temp_name, conn)

                conn.execute(sa.text(senttools.update_review_tbl_query(sentiment_temp_name)))
                completed_rows = senttools.count_completed(sentiment_temp_name, conn)

                input_reviewtext = str(raw_reviews.to_list())[1:-1]
                conn.execute(sa.text(senttools.delete_completed_from_temp(review_temp_name, input_reviewtext)))

            completed += completed_rows
        else:
            try_count += 1
            num_rows -= 10
            continue

    elif try_count == 3:
        failed += num_rows
        # skip the batch if failed 3x
        offset += num_rows

    try_count = 1
    num_rows = DEFAULT_NUM_ROWS

    flush = False if remaining <= num_rows else True
    aitools.print_result(completed, remaining, failed, flush)   
   

