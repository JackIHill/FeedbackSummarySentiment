import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.base import Connection

from typing import Optional

class VenueSummary():
    def __init__(self):
        self.table_name = 'Venue'
        self.primary_key = 'VenueID'
        
    def get_count_remaining(
        self,
        conn: Connection,
        start_date_id: int,
        end_date_id: int) -> int:

        query =  f"""
            SELECT count(distinct(r.VenueID))
            FROM Review r
            INNER JOIN (
                SELECT DISTINCT v.VenueID
                FROM Venue v
                LEFT JOIN Review r1 on r1.VenueID = v.VenueID
                WHERE r1.VenueID is not null
            ) x ON x.VenueID = r.VenueID
            INNER JOIN #DateRange dr on dr.StartDateID = {start_date_id} AND dr.EndDateID = {end_date_id}
            LEFT JOIN #SummaryVenue sv on sv.VenueID = r.VenueID and sv.DateRangeID = dr.DateRangeID
            WHERE 
                ReviewText IS NOT NULL AND 
                sv.VenueSummaryID IS NULL
                AND r.Review_DateID BETWEEN {start_date_id} and {end_date_id}
                AND NOT EXISTS (
                    SELECT * FROM Summary_Venue s_v
                    WHERE x.VenueID = s_v.VenueID
                    AND s_v.DateRangeID = dr.DateRangeID
                )
            """
        
        count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())
        return count

    def get_remaining_rows(
        self,
        conn: Connection,
        offset: Optional[int],
        start_date_id: int,
        end_date_id: int
        ) -> pd.DataFrame:

        query =  f"""
            WITH cte as (
                SELECT DISTINCT v.VenueID, ReviewID
                FROM Venue v
                LEFT JOIN Review r1 on r1.VenueID = v.VenueID
                INNER JOIN #DateRange dr on dr.StartDateID = {start_date_id} AND dr.EndDateID = {end_date_id}
                LEFT JOIN #SummaryVenue sv on sv.VenueID = r1.VenueID and sv.DateRangeID = dr.DateRangeID
                WHERE r1.ReviewText IS NOT NULL
                    AND sv.VenueSummaryID IS NULL
                    AND r1.Review_DateID BETWEEN {start_date_id} and {end_date_id}
                    AND NOT EXISTS (
                        SELECT * FROM Summary_Venue s_v
                        WHERE v.VenueID = s_v.VenueID
                          AND s_v.DateRangeID = dr.DateRangeID
                    )
            )
            SELECT r.VenueID,
                    ReviewText,
                    max(r.Review_DateID) Review_DateID
            FROM Review r
            INNER JOIN cte ON cte.ReviewID = r.ReviewID
            WHERE r.VenueID = (
                SELECT DISTINCT VenueID FROM cte ORDER BY cte.VenueID
                OFFSET {offset} ROWS
                FETCH NEXT 1 ROWS ONLY)
            GROUP BY r.VenueID, ReviewText     
            """

        rows = pd.read_sql(sa.text(query), conn)

        return rows

    def temp_insert(
        self,
        start_date_id: int,
        end_date_id: int,
        venueid: int,
        id2: Optional[int] = None) -> str:

        query = f"""
                INSERT INTO #SummaryVenue (VenueSummary, VenuePros, VenueCons, DateRangeID, VenueID, RegionSummaryID, OperatorSummaryID)
                SELECT t.Summary, t.Pros, t.Cons, dr.DateRangeID, {venueid}, 0, 0
                FROM #temp t
                INNER JOIN #DateRange dr on dr.StartDateID = {start_date_id} and dr.EndDateID = {end_date_id}
                WHERE NOT EXISTS (
                        SELECT * FROM #SummaryVenue
                        WHERE #SummaryVenue.VenueID = {venueid} AND
                            #SummaryVenue.DateRangeID = dr.DateRangeID
                        )
                """
        return query


class OperatorSummary():
    def __init__(self):
        self.table_name = 'Operator'
        self.value_field = 'OperatorName'
        self.primary_key = 'OperatorID'
        
    def get_count_remaining(
        self,
        conn: Connection,
        start_date_id: int,
        end_date_id: int) -> int:

        query = f"""
            SELECT count(distinct(rem_op.OperatorID)) from (
                    select VenueSummary, o.OperatorID, sv.DateRangeID
                    from #SummaryVenue sv
                    inner join Venue v on v.VenueID = sv.VenueID
                    inner join Brand b on b.BrandID = v.BrandID
                    inner join Operator o on o.OperatorID = b.OperatorID
                    LEFT JOIN #SummaryOperator so on so.OperatorID = o.OperatorID
                                                 and so.DateRangeID = sv.DateRangeID
                    WHERE so.OperatorID is null
                ) rem_op
            """
        count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())
        return count


    def get_remaining_rows(
        self,
        conn: Connection,
        offset: int,
        start_date_id: int,
        end_date_id: int
        ) -> pd.DataFrame:

        query = f"""
            WITH cte as (
                select VenueSummary, o.OperatorID, sv.DateRangeID
                from #SummaryVenue sv
                inner join Venue v on v.VenueID = sv.VenueID
                inner join Brand b on b.BrandID = v.BrandID
                inner join Operator o on o.OperatorID = b.OperatorID
                LEFT JOIN #SummaryOperator so on so.OperatorID = o.OperatorID and so.DateRangeID = sv.DateRangeID
                WHERE so.OperatorID IS NULL
            )
            select distinct * from cte
            where cte.OperatorID = 
                (SELECT distinct OperatorID FROM cte ORDER BY cte.OperatorID
                OFFSET {offset} ROWS
                FETCH NEXT 1 ROWS ONLY) 
            """

        rows = pd.read_sql(sa.text(query), conn)
        return rows


    def temp_insert(
        self,
        start_date_id: int,
        end_date_id: int,
        operatorid: int,
        id2: Optional[int] = None) -> str:

        query = f"""
                INSERT INTO #SummaryOperator (OperatorSummary, OperatorPros, OperatorCons, DateRangeID, OperatorID)
                SELECT t.Summary, t.Pros, t.Cons, dr.DateRangeID, {operatorid}
                FROM #temp t
                INNER JOIN #DateRange dr on dr.StartDateID = {start_date_id} and dr.EndDateID = {end_date_id}
                WHERE NOT EXISTS (
                        SELECT * FROM #SummaryOperator
                        WHERE #SummaryOperator.OperatorID = {operatorid} AND
                            #SummaryOperator.DateRangeID = dr.DateRangeID
                        )
                """
        return query


class RegionSummary():
    def __init__(self):
        self.table_name = 'Region'
        self.value_field = 'Region'
        self.primary_key = 'RegionID'
        self.foreign_key = 'OperatorID'

    def get_count_remaining(
        self,
        conn: Connection,
        start_date_id: int,
        end_date_id: int) -> int:

        query = f"""
            WITH cte AS (
            select distinct operatorid, regionid from (
                    select v.VenueID, VenueSummary, o.OperatorID, sv.DateRangeID, reg.RegionID
                    from #SummaryVenue sv
                    inner join Venue v on v.VenueID = sv.VenueID
                    inner join Brand b on b.BrandID = v.BrandID
                    inner join Operator o on o.OperatorID = b.OperatorID
                    inner join Postcode p on p.PostcodeID = v.PostcodeID
                    inner join Region reg on reg.RegionID = p.RegionID
                    LEFT JOIN #SummaryRegion sr on sr.RegionID = reg.RegionID
                         and sr.OperatorID = o.OperatorID 
                         and sr.DateRangeID = sv.DateRangeID
                    WHERE sr.RegionSummaryID IS NULL
                ) reg_op
            )
            select count(*) from cte
            """
        
        count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())
        return count


    def get_remaining_rows(
        self,
        conn: Connection,
        offset: int,
        start_date_id: int,
        end_date_id: int
        ) -> pd.DataFrame:

        query = f"""
            WITH cte as (
                select v.VenueID, VenueSummary, o.OperatorID, sv.DateRangeID, reg.RegionID from #SummaryVenue sv
                inner join Venue v on v.VenueID = sv.VenueID
                inner join Brand b on b.BrandID = v.BrandID
                inner join Operator o on o.OperatorID = b.OperatorID
                inner join Postcode p on p.PostcodeID = v.PostcodeID
                inner join Region reg on reg.RegionID = p.RegionID
                LEFT JOIN #SummaryRegion sr on sr.RegionID = reg.RegionID
                                          and sr.OperatorID = o.OperatorID
                                          and sr.DateRangeID = sv.DateRangeID
                WHERE sr.RegionSummaryID IS NULL
            )
            select VenueID, VenueSummary, x.OperatorID, DateRangeID, x.RegionID from cte
            inner join (
                select distinct operatorid, regionid
                from cte
                order by operatorid, regionid
                OFFSET {offset} ROWS
                FETCH NEXT 1 ROWS ONLY
            ) x
            on cte.OperatorID = x.OperatorID and cte.RegionID = x.RegionID
            """
        
        rows = pd.read_sql(sa.text(query), conn)  
        return rows


    def temp_insert(
        self,
        start_date_id: int,
        end_date_id: int,
        regionid: int,
        operatorid: int) -> str:

        query = f"""
            INSERT INTO #SummaryRegion (RegionSummary, RegionPros, RegionCons, DateRangeID, RegionID, OperatorID)
            SELECT t.Summary, t.Pros, t.Cons, dr.DateRangeID, {regionid}, {operatorid}
            FROM #temp t
            INNER JOIN #DateRange dr on dr.StartDateID = {start_date_id} and dr.EndDateID = {end_date_id}
            WHERE NOT EXISTS (
                    SELECT * FROM #SummaryRegion
                    WHERE #SummaryRegion.OperatorID = {operatorid} AND
                        #SummaryRegion.RegionID = {regionid} AND
                        #SummaryRegion.DateRangeID = dr.DateRangeID
                    )
            """
        return query


def summary_prompt(json, category: str) -> str:
    prompt = f"""

        There are 3 categories: Venue, Region, and Operator.
        The following JSON contains restaurant reviews (ReviewText) for the category: {category}.
        Each review is a separate entry.
        
        A) Write a concise, up to 50 word summary of the reviews for that category. 
           Requirements:
           1) Only refer to the opinions and sentiments in the reviews.
           2) Don't mention anything regarding closure or opening of a venue if that is the category.
           3) Don't mention the names of people. Be as objective and holistic as possible.
           4) Do not mention a category that is not the category '{category}' in the summary.
           5) If the category is not 'Venue', then the summary should refer to the venues WITHIN the category.

        B) Then, provide a short-form list of up to 3 pros based on the same reviews.
            Requirements:
            1) After each item, list the % of reviews that reference that pro to the nearest 10%.
            only count explicit mentions towards the percentage (e.g., "staff were great" counts, but vague compliments like "it was great" do not unless directly tied to a category).
            Count each pro only once per review, even if mentioned multiple times in the same review.
            2) Each item should follow the structure 'adjective + noun', e.g., great staff (20%) or nice environment (60%).
            3) Group related terms/adjectives under one category where appropriate. For example:
                If "great staff" is mentioned in one review and "friendly workers" in another, combine them under 'great staff'.
            4) Ensure groupings are logical, objective, and not overly broad.
            5) If there are no identifiable pros, return only '-'.
            6) There should be no more than 1 row in the output. If there is, retry.

        C) Repeat the process for up to 3 cons, using the same structure and approach.

        Here are the reviews: \n\n{json}
        """
    return prompt


def date_range_insert(start_date_id, end_date_id, months):
    query = f"""
            INSERT INTO #DateRange (StartDateID, EndDateID, Months)
            SELECT {start_date_id}, {end_date_id}, {months}
            WHERE NOT EXISTS (SELECT * FROM #DateRange dr
                              WHERE dr.StartDateID = {start_date_id} AND
                                    dr.EndDateID = {end_date_id} AND
                                    dr.Months = {months}
                            )
            """
    return query


def final_insert() -> str:
    query = """
            INSERT INTO DateRange (StartDateID, EndDateID, Months)
            SELECT StartDateID, EndDateID, Months FROM #DateRange d_r     
            WHERE NOT EXISTS (SELECT * FROM DateRange dr
                            WHERE d_r.StartDateID = dr.StartDateID AND 
                                  d_r.EndDateID = dr.EndDateID AND
                                  d_r.Months = dr.Months)      


            INSERT INTO Summary_Operator (OperatorSummary, OperatorPros, OperatorCons, DateRangeID, OperatorID)
            SELECT OperatorSummary, OperatorPros, OperatorCons, DateRangeID, OperatorID
            FROM #SummaryOperator so
            WHERE NOT EXISTS (SELECT * FROM Summary_Operator s_o
                            WHERE s_o.DateRangeID = so.DateRangeID AND
                                  s_o.OperatorID = so.OperatorID)                         

                                    
            INSERT INTO Summary_Region (RegionSummary, RegionPros, RegionCons, DateRangeID, RegionID, OperatorID)
            SELECT RegionSummary, RegionPros, RegionCons, DateRangeID, RegionID, OperatorID
            FROM #SummaryRegion sr                    
            WHERE NOT EXISTS (SELECT * FROM Summary_Region s_r
                            WHERE s_r.DateRangeID = sr.DateRangeID AND
                                  s_r.OperatorID = sr.OperatorID AND
                                  s_r.RegionID = sr.RegionID)                         

                                            
            INSERT INTO Summary_Venue (VenueSummary, VenuePros, VenueCons, DateRangeID, VenueID, RegionSummaryID, OperatorSummaryID) 
            SELECT sv.VenueSummary, sv.VenuePros, sv.VenueCons, sv.DateRangeID, sv.VenueID, sr.RegionSummaryID, so.OperatorSummaryID
            FROM #SummaryVenue sv
            INNER JOIN Venue v ON v.VenueID = sv.VenueID
            INNER JOIN Brand b ON b.BrandID = v.BrandID
            INNER JOIN Operator o ON o.OperatorID = b.OperatorID
            INNER JOIN Postcode p ON p.PostcodeID = v.PostcodeID                     
            INNER JOIN Region r ON r.RegionID = p.RegionID
            INNER JOIN #SummaryRegion sr ON sr.RegionID = r.RegionID AND
                                            sr.OperatorID = o.OperatorID AND
                                            sr.DateRangeID = sv.DateRangeID
            INNER JOIN #SummaryOperator so ON so.OperatorID = o.OperatorID AND
                                            so.DateRangeID = sv.DateRangeID
            WHERE NOT EXISTS (SELECT * FROM Summary_Venue s_v
                            WHERE s_v.DateRangeID = sv.DateRangeID AND
                                  s_v.VenueID = sv.VenueID)          
                  
            """
    return query


def get_unique_ids(tbl: pd.DataFrame, id_col_name: str) -> int:
    return int(tbl[id_col_name].unique()[0])


def getid_fromvalue(obj, value: str, conn: Connection) -> int:
    query = sa.text(f"select {obj.primary_key} from {obj.table_name} where {obj.value_field} = '{value}'")
    return int(pd.read_sql(query, conn)[f'{obj.primary_key}'][0])


JSON_FORMAT = {
        "type": "json_schema",
        "json_schema": {
            "name": "review_summary",
            "schema": {
                "type": "object",
                    "properties": {
                        "review_summary": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "Summary": {
                                        "type": "string"
                                    },
                                    "Pros": {
                                        "type": "string"
                                    },
                                    "Cons": {
                                        "type": "string"
                                    }
                                },
                                "required": [
                                    "Summary",
                                    "Pros",
                                    "Cons"
                                    ],
                                "additionalProperties": False
                            
                            } 
                    }
                },
                "required": ["review_summary"],
                "additionalProperties": False
            }  
            ,"strict": True
        }       
    }
