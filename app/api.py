

from flask import Blueprint, request, jsonify, session
import sqlalchemy as sa
from sqlalchemy.engine.base import Connection, Engine
from openai import OpenAI
import numbers
from decimal import Decimal
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from credentials.SQL_Credentials import username, password, server, database, driver

def establish_connection(
        sql_user: str,
        sql_pass: str,
        sql_server: str,
        sql_db: str,
        sql_driver: str,
        num_workers: int
        ) -> Engine:
    
    connection_url = f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_server}/{sql_db}?driver={sql_driver}"
    engine = sa.create_engine(
        connection_url,
        pool_size = num_workers * 2,
        max_overflow = 30,
        pool_timeout=30,
        pool_pre_ping=True)
    
    return engine

api = Blueprint('api', __name__)

engine = establish_connection(
    username,
    password,
    server,
    database,
    driver,
    1
    )


def bind_in_clause(column_name: str, values: list[str], param_prefix: str = "val") -> tuple[str, dict]:
    """
    SQLAlchemy-compatible IN clause // parameter mapping.

    Example:
        bind_in_clause("o.OperatorName", ["KFC", "Nandos"])
        -> ("o.OperatorName IN (:val0, :val1)", {"val0": "KFC", "val1": "Nandos"})
    """
    placeholders = [f":{param_prefix}{i}" for i in range(len(values))]
    clause = f"{column_name} IN ({', '.join(placeholders)})"
    params = {f"{param_prefix}{i}": val for i, val in enumerate(values)}
    return clause, params


def stats_ready_for_JSON(list_of_dicts: list[dict, dict]):
    for stat_pair in list_of_dicts:
        for key, value in stat_pair.items():
            if isinstance(value, numbers.Number):
                # SQL output -> max 2 decimal places unless specified
                if isinstance(value, Decimal):
                    exponent = value.as_tuple().exponent
                    dp = 2 if exponent <= -2 else 1 if exponent < 0 else None

                stat_pair[key] = (
                    f"{int(value):,}" if value == int(value) # no decimal places if whole number
                    else f"{value:,.{dp}f}" # :,1f -> thousands separator, 1 dp. 
                )

    return list_of_dicts


def get_all_operators():
    query = sa.text(f"""
        SELECT DISTINCT o.OperatorID, o.OperatorName 
        FROM Operator o
        INNER JOIN Brand b ON b.OperatorID = o.OperatorID
        INNER JOIN Venue v ON v.BrandID = b.BrandID
        INNER JOIN Review r ON r.VenueID = v.VenueID
        WHERE r.ReviewID IS NOT NULL
        ORDER BY o.OperatorName
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        data = [dict(row._mapping) for row in result]

    return data

def get_all_phrases():
    query = sa.text(f"""
        SELECT DISTINCT Phrase from Phrase_SentimentFlag
        ORDER BY Phrase
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        data = [dict(row._mapping) for row in result]

    return data


def get_selected_operators_clause():
    selected_operators = session.get('selected_operators', [])
    selected_IDs = [op['OperatorID'] for op in selected_operators]

    operator_in_these, params = bind_in_clause("o.OperatorID", selected_IDs, "op")
    
    return operator_in_these, params




@api.route('/api/infostats/overall_sentiment')
def infostats_overall_sentiment():    
    current_operatorID_in_rotation = request.args.get('current_operatorID_in_rotation')

    query = sa.text(f"""
    WITH PhraseSentimentScores AS (
        SELECT
            o.OperatorID,
            psf.Phrase,
            AVG(s.SentimentScore) AS AvgSentimentScore
        FROM operator o
        LEFT JOIN brand b ON b.OperatorID = o.OperatorID
        LEFT JOIN venue v ON v.BrandID = b.BrandID
        LEFT JOIN review r ON r.VenueID = v.VenueID AND r.ReviewText IS NOT NULL
        LEFT JOIN Phrase_SentimentReview psr ON psr.ReviewID = r.ReviewID
        LEFT JOIN Phrase_SentimentFlag psf ON psf.PhraseSentimentFlagID = psr.PhraseSentimentFlagID AND psf.PhraseFlag = 1
        LEFT JOIN Sentiment s ON s.SentimentID = psf.PhraseSentimentID
        WHERE o.OperatorID = :current_operatorID_in_rotation
        GROUP BY o.OperatorID, psf.Phrase
    ),
    RankedPhrases AS (
        SELECT
            OperatorID,
            Phrase,
            AvgSentimentScore,
            ROW_NUMBER() OVER (ORDER BY AvgSentimentScore DESC) AS PositiveRank,
            ROW_NUMBER() OVER (ORDER BY AvgSentimentScore ASC) AS NegativeRank
        FROM PhraseSentimentScores
        WHERE AvgSentimentScore IS NOT NULL
    ),
    TopBottomPhrases AS (
        SELECT
            MAX(CASE WHEN PositiveRank = 1 THEN Phrase END) AS MostPositivePhraseName,
            MAX(CASE WHEN PositiveRank = 1 THEN AvgSentimentScore END) AS MostPositivePhraseSentiment,
            MAX(CASE WHEN NegativeRank = 1 THEN Phrase END) AS MostNegativePhraseName,
            MAX(CASE WHEN NegativeRank = 1 THEN AvgSentimentScore END) AS MostNegativePhraseSentiment
        FROM RankedPhrases
    )

    SELECT
        o.OperatorName,

        COALESCE(CAST(ROUND(
            COUNT(CASE WHEN s.SentimentText != 'Unknown - Unprocessed' THEN 1 END) * 100.0 / NULLIF(COUNT(r.ReviewID), 0),
            1
        ) AS DECIMAL(32,1)), 0) AS ProcessedReviewRatio,

        CAST(ROUND(
            AVG(CASE WHEN s.SentimentText != 'Unknown - Unprocessed' THEN s.SentimentScore END),
            2
        ) AS DECIMAL(32,2)) AS AvgSentimentScore,

        CAST(COUNT(CASE WHEN s.SentimentScore > 0 THEN 1 END) * 100.0 /
            NULLIF(COUNT(CASE WHEN s.SentimentText != 'Unknown - Unprocessed' THEN 1 END), 0)
        AS DECIMAL(5,1)) AS PositiveReviewPercentage,

        COUNT(r.ReviewID) AS CountReviews,

        tbp.MostPositivePhraseName,
        tbp.MostPositivePhraseSentiment,
        tbp.MostNegativePhraseName,
        tbp.MostNegativePhraseSentiment

    FROM operator o
    LEFT JOIN brand b ON b.OperatorID = o.OperatorID
    LEFT JOIN venue v ON v.BrandID = b.BrandID
    LEFT JOIN review r ON r.VenueID = v.VenueID AND r.ReviewText IS NOT NULL
    LEFT JOIN sentiment s ON s.SentimentID = r.ReviewSentimentID

    CROSS JOIN TopBottomPhrases tbp

    WHERE o.OperatorID = :current_operatorID_in_rotation

    GROUP BY o.OperatorName,
            tbp.MostPositivePhraseName, tbp.MostPositivePhraseSentiment,
            tbp.MostNegativePhraseName, tbp.MostNegativePhraseSentiment

    ORDER BY o.OperatorName



    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"current_operatorID_in_rotation": current_operatorID_in_rotation})
        data = [dict(row._mapping) for row in result]
        data = stats_ready_for_JSON(data)
        

    return data


@api.route('/api/graph/reviewrating_by_overall_sentiment_data')
def get_reviewrating_by_overall_sentiment_data():
    operator_in_these, params = get_selected_operators_clause()

    query = sa.text(f"""
        SELECT TOP 10
            o.OperatorName,
            CAST(ROUND(AVG(r.ReviewRating), 2) AS NUMERIC(36, 2)) AS AvgReviewRating,
            CAST(ROUND(AVG(s.SentimentScore), 2) AS NUMERIC(36, 2)) AS AvgSentimentScore
                    
        FROM review r
        INNER JOIN Venue v ON v.VenueID = r.VenueID
        INNER JOIN brand b ON b.BrandID = v.BrandID
        INNER JOIN operator o ON o.OperatorID = b.OperatorID
        INNER JOIN sentiment s on s.SentimentID = r.ReviewSentimentID
        WHERE {operator_in_these} and s.SentimentScore IS NOT NULL
        GROUP BY o.OperatorName
        ORDER BY AvgReviewRating DESC
    """)

    with engine.connect() as conn:
        result = conn.execute(query, params)
        data = [dict(row._mapping) for row in result]
    
    # need date filter too..... 

    return jsonify(data)


@api.route('/api/graph/overall_sentiment_portion')
def get_overall_sentiment_portion():
    operator_in_these, params = get_selected_operators_clause()

    query = sa.text(f"""
        SELECT
            s.SentimentScore,
            count(1) CountSentimentScore
        FROM review r
        INNER JOIN Venue v ON v.VenueID = r.VenueID
        INNER JOIN brand b ON b.BrandID = v.BrandID
        INNER JOIN operator o ON o.OperatorID = b.OperatorID
        INNER JOIN sentiment s on s.SentimentID = r.ReviewSentimentID
        WHERE {operator_in_these} and s.SentimentScore IS NOT NULL
        GROUP BY s.SentimentScore
        --ORDER BY AvgReviewRating DESC
    """)

    with engine.connect() as conn:
        result = conn.execute(query, params)
        data = [dict(row._mapping) for row in result]
    
    return jsonify(data)




@api.route('/api/graph/AvgSentimentOverTime')
def get_average_sentiment_over_time():
    operator_in_these, params = get_selected_operators_clause()

    query = sa.text(f"""
        SELECT 
        ReviewYear,
        ReviewRating,
        COUNT(*) * 1.0 / TotalReviews AS RatingProportion
        FROM (
        SELECT 
            YEAR(d.ActualDate) AS ReviewYear,
            r.ReviewRating,
            COUNT(*) OVER (PARTITION BY YEAR(d.ActualDate)) AS TotalReviews
        FROM review r
        INNER JOIN Venue v ON v.VenueID = r.VenueID
        INNER JOIN brand b ON b.BrandID = v.BrandID
        INNER JOIN operator o ON o.OperatorID = b.OperatorID
        INNER JOIN Dates d ON d.DateID = r.Review_DateID
        WHERE {operator_in_these}
            AND r.ReviewRating IS NOT NULL
        ) AS base
        GROUP BY ReviewYear, ReviewRating, TotalReviews
        ORDER BY ReviewYear, ReviewRating;
    """)

    with engine.connect() as conn:
        result = conn.execute(query, params)
        data = [dict(row._mapping) for row in result]
    return jsonify(data)


@api.route('/api/graph/reviewrating_by_overall_sentiment')
def get_reviewrrating_by_overall_sentiment():
    operator_in_these, params = get_selected_operators_clause()

    query = sa.text(f"""
        SELECT
            s.SentimentScore,
            avg(r.ReviewRating) AvgReviewRating
        FROM review r
        INNER JOIN Venue v ON v.VenueID = r.VenueID
        INNER JOIN brand b ON b.BrandID = v.BrandID
        INNER JOIN operator o ON o.OperatorID = b.OperatorID
        INNER JOIN sentiment s on s.SentimentID = r.ReviewSentimentID
        WHERE {operator_in_these} and s.SentimentScore IS NOT NULL
        GROUP BY s.SentimentScore
        --ORDER BY AvgReviewRating DESC
    """)

    with engine.connect() as conn:
        result = conn.execute(query, params)
        data = [dict(row._mapping) for row in result]
    
    return jsonify(data)