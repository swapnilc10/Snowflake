CREATE DATABASE s3_data;

USE DATABASE s3_data;

USE SCHEMA public;

USE WAREHOUSE COMPUTE_WH;

---> create the storage integration

CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = "arn:aws:iam::246636109038:role/snowflake_role_snowpipe"
  STORAGE_ALLOWED_LOCATIONS = ("s3://bucket1swap/Snowflake_pipe/");

---> describe the storage integration to see the info to copy over to AWS
DESCRIBE INTEGRATION S3_role_integration;



CREATE OR REPLACE TABLE S3_YOUTUBE_TABLE(VIDEO_ID STRING, PUBLISHED_DATE DATE,VIDEO_TITLE STRING,
                                VIDEO_OWNER_CHANNEL_TITLE STRING,THUMBNAIL STRING, TOTAL_NUMBER_OF_VIEWS NUMBER,
                                URL STRING,TOTAL_LIKES NUMBER, TOTAL_COMMENTS NUMBER,VIDEO_CATEGORY_ID NUMBER,
                                CATEGORY_VALUE STRING);


---> creating stage with the link to the S3 bucket and info on the associated storage integration

CREATE OR REPLACE STAGE S3_stage
  url = 's3://bucket1swap/Snowflake_pipe/'
  storage_integration = S3_role_integration;


SHOW STAGES;


---> To see list of all the pipes
SHOW PIPES;

DESCRIBE PIPE S3_data.public.S3_pipe;

---> see the files in the stage
LIST @S3_stage;

---> select the first two columns from the stage
SELECT $1,$2, $3, $4 FROM @S3_stage;



CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;


---> create the snowpipe, copying from S3_stage into S3_table
CREATE OR REPLACE PIPE S3_data.public.S3_pipe AUTO_INGEST=TRUE AS
  COPY INTO S3_data.public.S3_YOUTUBE_TABLE
  FROM @S3_data.public.S3_stage
  FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

SELECT * FROM S3_data.public.S3_YOUTUBE_TABLE;



---> pause the pipe
--ALTER PIPE S3_data.public.S3_pipe SET PIPE_EXECUTION_PAUSED = TRUE;





