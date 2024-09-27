import pandas as pd
import sqlalchemy as sa

class VenueSummary():
    def __init__(self):
        self.table = 'Venue'
        self.idcol = 'VenueID'
        
    def get_count_remaining(self, conn, date_int, date_string=None):
        query =  f"""
            SELECT count(distinct(r.VenueID))
            FROM Review r
            INNER JOIN (
                SELECT DISTINCT v.VenueID
                FROM Venue v
                LEFT JOIN Review r1 on r1.VenueID = v.VenueID
                WHERE r1.VenueID is not null
            ) x ON x.VenueID = r.VenueID
            INNER JOIN Dates rev on rev.DateID = r.Review_DateID
            LEFT JOIN #SummaryVenue sv on sv.VenueID = r.VenueID and sv.DateID = {date_int}
            WHERE ReviewText IS NOT NULL AND 
                sv.VenueSummaryID IS NULL AND
                YEAR(rev.ActualDate) = YEAR('{date_string}') and MONTH(rev.ActualDate) = MONTH('{date_string}')

            """
        
        count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())
        return count

    def get_remaining_rows(self, conn, date_int, date_string=None, offset=None):
        query =  f"""
            WITH cte as (
                SELECT DISTINCT v.VenueID, ReviewID
                FROM Venue v
                LEFT JOIN Review r1 on r1.VenueID = v.VenueID
                LEFT JOIN #SummaryVenue sv on sv.VenueID = v.VenueID and sv.DateID = {date_int}
                INNER JOIN Dates rev on rev.DateID = Review_DateID
                WHERE r1.ReviewText IS NOT NULL
                    AND sv.VenueSummaryID IS NULL
                    AND YEAR(rev.ActualDate) = YEAR('{date_string}')
                    AND MONTH(rev.ActualDate) = MONTH('{date_string}')
            )
            SELECT r.VenueID,
                    ReviewText,
                    max(r.Review_DateID) Review_DateID
            FROM Review r
            INNER JOIN cte ON cte.ReviewID = r.ReviewID
            WHERE r.VenueID = (
                SELECT VenueID FROM cte ORDER BY cte.VenueID
                OFFSET {offset} ROWS
                FETCH NEXT 1 ROWS ONLY)
            GROUP BY r.VenueID, ReviewText     
            """

        rows = pd.read_sql(sa.text(query), conn)

        return rows

    def temp_insert(self, date_string, venueid, id2=None):
        query = f"""
                INSERT INTO #SummaryVenue (VenueSummary, DateID, VenueID, RegionSummaryID, OperatorSummaryID)
                SELECT t.Summary, d.DateID, {venueid}, 0, 0
                FROM #temp t
                INNER JOIN Dates d on d.ActualDate = '{date_string}'
                WHERE NOT EXISTS (
                        SELECT * FROM #SummaryVenue
                        WHERE #SummaryVenue.VenueID = {venueid} AND
                            #SummaryVenue.DateID = d.DateID
                        )
                """
        return query


class OperatorSummary():
    def __init__(self):
        self.table = 'Operator'
        self.idcol = 'OperatorID'
        
    def get_count_remaining(self, conn, date_int, date_string):
        query = f"""
            SELECT count(distinct(rem_op.OperatorID)) from (
                    select VenueSummary, o.OperatorID, sv.DateID
                    from #SummaryVenue sv
                    inner join Venue v on v.VenueID = sv.VenueID
                    inner join Brand b on b.BrandID = v.BrandID
                    inner join Operator o on o.OperatorID = b.OperatorID
                    LEFT JOIN #SummaryOperator so on so.OperatorID = o.OperatorID and sv.DateID = {date_int}
                    WHERE so.OperatorID is null
                ) rem_op
            """
        count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())
        return count


    def get_remaining_rows(self, conn, date_int, date_string, offset):
        query = f"""
            WITH cte as (
                select VenueSummary, o.OperatorID, sv.DateID
                from #SummaryVenue sv
                inner join Venue v on v.VenueID = sv.VenueID
                inner join Brand b on b.BrandID = v.BrandID
                inner join Operator o on o.OperatorID = b.OperatorID
                LEFT JOIN #SummaryOperator so on so.OperatorID = o.OperatorID and sv.DateID = {date_int}
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


    def temp_insert(self, date_string, operatorid, id2=None):
        query = f"""
                INSERT INTO #SummaryOperator (OperatorSummary, DateID, OperatorID)
                SELECT t.Summary, d.DateID, {operatorid}
                FROM #temp t
                INNER JOIN Dates d on d.ActualDate = '{date_string}'
                WHERE NOT EXISTS (
                        SELECT * FROM #SummaryOperator
                        WHERE #SummaryOperator.OperatorID = {operatorid} AND

                            #SummaryOperator.DateID = d.DateID
                        )
                """
        return query


class RegionSummary():
    def __init__(self):
        self.table = 'Region'
        self.idcol = 'RegionID'
        self.idcol2 = 'OperatorID'

    def get_count_remaining(self, conn, date_int, date_string):
        query = f"""
            WITH cte AS (
            select distinct operatorid, regionid from (
                    select v.VenueID, VenueSummary, o.OperatorID, sv.DateID, reg.RegionID from #SummaryVenue sv
                    inner join Venue v on v.VenueID = sv.VenueID
                    inner join Brand b on b.BrandID = v.BrandID
                    inner join Operator o on o.OperatorID = b.OperatorID
                    inner join Postcode p on p.PostcodeID = v.PostcodeID
                    inner join Region reg on reg.RegionID = p.RegionID and Region <> '-'
                    LEFT JOIN #SummaryRegion sr on sr.RegionID = reg.RegionID
                         and sr.OperatorID = o.OperatorID 
                         and sv.DateID = {date_int}
                    WHERE sr.RegionSummaryID IS NULL
                ) reg_op
            )
            select count(*) from cte
            """
        
        count = int(pd.read_sql(sa.text(query), conn).to_string(index=False).strip())
        return count


    def get_remaining_rows(self, conn, date_int, date_string, offset):
        query = f"""
            WITH cte as (
                select v.VenueID, VenueSummary, o.OperatorID, sv.DateID, reg.RegionID from #SummaryVenue sv
                inner join Venue v on v.VenueID = sv.VenueID
                inner join Brand b on b.BrandID = v.BrandID
                inner join Operator o on o.OperatorID = b.OperatorID
                inner join Postcode p on p.PostcodeID = v.PostcodeID
                inner join Region reg on reg.RegionID = p.RegionID and Region <> '-'
                LEFT JOIN #SummaryRegion sr on sr.RegionID = reg.RegionID and sv.DateID = {date_int}
                WHERE sr.RegionSummaryID IS NULL
            )
            select VenueID, VenueSummary, x.OperatorID, DateID, x.RegionID from cte
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


    def temp_insert(self, date_string, regionid, operatorid):
        query = f"""
            INSERT INTO #SummaryRegion (RegionSummary, DateID, RegionID, OperatorID)
            SELECT t.Summary, d.DateID, {regionid}, {operatorid}
            FROM #temp t
            INNER JOIN Dates d on d.ActualDate = '{date_string}'
            WHERE NOT EXISTS (
                    SELECT * FROM #SummaryRegion
                    WHERE #SummaryRegion.OperatorID = {operatorid} AND
                        #SummaryRegion.RegionID = {regionid} AND
                        #SummaryRegion.DateID = d.DateID
                    )
            """
        return query


def summary_prompt(json, category):
    prompt = f"""
        The following JSON contains restaurant reviews (ReviewText) for a {category}.
        Each review is a separate entry.
        Write a concise, up to 50 word summary of the reviews for the {category}. Only refer to the opinions
        and sentiments in the reviews, and disregard the {category} name.
        Don't mention anything regarding closure or opening of the {category}.
        Don't mention names. Be as objective as possible.

        Here are the reviews: \n\n{json}
        """
    return prompt


def final_insert(conn):
    query = """
            INSERT INTO Summary_Operator (OperatorSummary, DateID, OperatorID)
            SELECT OperatorSummary, DateID, OperatorID
            FROM #SummaryOperator so
            WHERE NOT EXISTS (SELECT * FROM Summary_Operator s_o
                            WHERE s_o.DateID = so.DateID AND
                                    s_o.OperatorID = so.OperatorID)                         

            INSERT INTO Summary_Region (RegionSummary, DateID, RegionID, OperatorID)
            SELECT RegionSummary, DateID, RegionID, OperatorID
            FROM #SummaryRegion sr                    
            WHERE NOT EXISTS (SELECT * FROM Summary_Region s_r
                            WHERE s_r.DateID = sr.DateID AND
                                    s_r.OperatorID = sr.OperatorID AND
                                    s_r.RegionID = sr.RegionID)                         


            INSERT INTO Summary_Venue (VenueSummary, DateID, VenueID, RegionSummaryID, OperatorSummaryID) 
            SELECT sv.VenueSummary, sv.DateID, sv.VenueID, sr.RegionSummaryID, so.OperatorSummaryID
            FROM #SummaryVenue sv
            INNER JOIN Venue v ON v.VenueID = sv.VenueID
            INNER JOIN Brand b ON b.BrandID = v.BrandID
            INNER JOIN Operator o ON o.OperatorID = o.OperatorID
            INNER JOIN Postcode p ON p.PostcodeID = v.PostcodeID                     
            INNER JOIN Region r ON r.RegionID = p.RegionID
            INNER JOIN #SummaryRegion sr ON sr.RegionID = r.RegionID AND
                                            sr.OperatorID = o.OperatorID AND
                                            sr.DateID = sv.DateID
            INNER JOIN #SummaryOperator so ON so.OperatorID = o.OperatorID AND
                                            so.DateID = sv.DateID
            WHERE NOT EXISTS (SELECT * FROM Summary_Venue s_v
                            WHERE s_v.DateID = sv.DateID AND
                                    s_v.VenueID = sv.VenueID)                                            
            """
    return query


def get_unique_ids(tbl, id_col_name):
    return int(tbl[id_col_name].unique()[0])


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
                                    }
                                },
                                "required": [
                                    "Summary"
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
