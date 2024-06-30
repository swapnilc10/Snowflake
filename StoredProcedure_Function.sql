import snowflake.snowpark as snowpark
#from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    session.sql('''CREATE SCHEMA IF NOT EXISTS ANALYTICS''').collect()
    session.sql('CREATE TABLE IF NOT EXISTS ANALYTICS.YOUTUBE_ANALYTICS (VIDEO_ID STRING, PUBLISHED_DATE DATE,VIDEO_TITLE STRING,VIDEO_OWNER_CHANNEL_TITLE STRING,THUMBNAIL STRING, TOTAL_NUMBER_OF_VIEWS NUMBER,URL STRING,TOTAL_LIKES NUMBER, TOTAL_COMMENTS NUMBER,VIDEO_CATEGORY_ID NUMBER,CATEGORY_VALUE STRING, RATIO NUMBER)').collect()
    session.sql('''INSERT INTO ANALYTICS.YOUTUBE_ANALYTICS
    SELECT yt.*,ceil((total_number_of_views*0.05)+(total_comments*0.03)+(total_likes*0.02)) as ratio
  FROM S3_DATA.PUBLIC.S3_YOUTUBE_TABLE as yt''').collect()
    dataframe = session.table('ANALYTICS.YOUTUBE_ANALYTICS')
    dataframe.show()

    return dataframe