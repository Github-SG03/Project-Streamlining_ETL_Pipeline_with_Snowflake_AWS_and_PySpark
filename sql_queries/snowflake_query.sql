-----------------AWS Setup with Snowflake Storage Integration------------------------------------------------------------------/////////////////////////////////////////////////////////////////////
--1.Create snowflake Storage Integration Object using following syntax:
CREATE STORAGE INTEGRATION aws_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767397752598:role/aws_s3_integration'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sgs-s3/');

  
--Run ‘DESCRIBE integration S3_INT’
DESCRIBE integration aws_s3_integration;

---Now in Snowflake run following query and copy statement part of the JSON and paste it in the last to the JSON we copied from JSON preview of SNS and update SNS and save({"Sid":"1","Effect":"Allow","Principal":{"AWS":"XXXXX"},"Action":["sns:Subscribe"],"Resource":["XXXXXXX"]})



------------Snowflake Objects Creation Part – 1-----------------------------------------------------------------------------------
--1. Create Staging Database to store schemas like table, pipes,stages....
Create or replace database ETL_PROJECT_STAGING;

--2. Create table to store covid-19 staging data
create or replace TABLE ETL_PROJECT_STAGING.PUBLIC.COVID_19 
( SNO NUMBER(38,0) autoincrement,
REPORTEDDATE VARCHAR(100),
TIME VARCHAR(100),
STATE VARCHAR(50),
CONFIRMEDINDIANNATIONAL NUMBER(38,0),
CONFIRMEDFOREIGNNATIONAL NUMBER(38,0),
CURED NUMBER(38,0), 
DEATHS NUMBER(38,0),
CONFIRMED NUMBER(38,0)
)
COMMENT='This is covid_19 India Data';

--3. Create File Format as we will be having csv file which needs to be loaded in table
Create or replace file format ETL_PROJECT_STAGING.PUBLIC.COVID_CSV
type = 'csv',
field_delimiter = ',',
record_delimiter = '\n',
skip_header = 1,
field_optionally_enclosed_by = '\042',null_if = ('\\N');

--4. Create an external staging object using S3 Bucket location and storage integration object
create or replace stage ETL_PROJECT_STAGING.PUBLIC.covid_s3_stage 
storage_integration = aws_s3_integration  
url = 's3://sgs-s3'  
file_format = ETL_PROJECT_STAGING.PUBLIC.COVID_CSV;
show stages;

--5.0. Create pipe object to ingest data automatically from S3 to table in snowflake whenever data gets landed to S3
CREATE OR REPLACE PIPE ETL_PROJECT_STAGING.PUBLIC.s3_to_snowflake_covid_19
AUTO_INGEST = TRUE
AS
COPY INTO ETL_PROJECT_STAGING.PUBLIC.COVID_19
FROM @ETL_PROJECT_STAGING.PUBLIC.covid_s3_stage
FILE_FORMAT = (FORMAT_NAME = 'ETL_PROJECT_STAGING.PUBLIC.covid_csv');

--5.1. See the pipe load status
SELECT * FROM INFORMATION_SCHEMA.LOAD_HISTORY
WHERE PIPE_NAME = 'ETL_PROJECT_STAGING.PUBLIC.s3_to_snowflake_covid_19'
AND LOAD_TIME > (CURRENT_TIMESTAMP - INTERVAL '1 hour');

--6. Validate whether all the objects got created successfully
Show Databases;
Show Schemas;
Show tables;
Show file formats;
Show stages;
Show Pipes;

--7. Check snowpipe status using query below. It should be in paused status
Select system$pipe_status('ETL_PROJECT_STAGING.PUBLIC.s3_to_snowflake_covid_19');

--8. Now resume the pipe status using below query
alter pipe ETL_PROJECT_STAGING.PUBLIC.s3_to_snowflake_covid_19 set pipe_execution_paused = true;//false


--10. Create table to store covid-19 final data
create or replace TABLE ETL_PROJECT_FINAL.PUBLIC.COVID_19 (
SNO NUMBER(38,0) autoincrement,
REPORTEDDATE VARCHAR(100),
TIME VARCHAR(100),
STATE VARCHAR(50),
CONFIRMEDINDIANNATIONAL NUMBER(38,0),
CONFIRMEDFOREIGNNATIONAL NUMBER(38,0),
CURED NUMBER(38,0), DEATHS NUMBER(38,0),
CONFIRMED NUMBER(38,0)
)
COMMENT='This is covid_19 India Data';



--------------------------Historical Data Load Verification---------------------------------------------------------------------------------
--0. Please verify the data load using below query
SELECT * FROM TABLE(information_schema.copy_history(table_name=>'ETL_PROJECT_STAGING.PUBLIC.COVID_19',start_time=>dateadd(hours,-1,current_timestamp())));

--1. Please verify the data in staging table using below query
Select * from ETL_PROJECT_STAGING.PUBLIC.COVID_19;

--2. Now move this historical data to final table using below query
Insert into ETL_PROJECT_FINAL.PUBLIC.COVID_19(SNO,REPORTEDDATE,TIME,STATE,CONFIRMEDINDIANNATIONAL,CONFIRMEDFOREIGNNATIONAL,CURED,DEATHS,CONFIRMED)    
Select * from ETL_PROJECT_STAGING.PUBLIC.COVID_19;

--3. Please verify the data in final table using below query
Select * from ETL_PROJECT_FINAL.PUBLIC.COVID_19;


------SnowFlake Objects Creation Part – 2:STAGING AND FINAL TABLE OBJECTS IN SNOWFLAKE--------------------------------------------------------------
--11.0 Create Stream to capture data change when any delta data gets loaded into staging table// Note : Make append_only = true to force system to add or update new records.
Create stream ETL_PROJECT_STAGING.PUBLIC.covid_19_stream on table ETL_PROJECT_STAGING.PUBLIC.COVID_19 append_only = true;

--11.1  Query to See any changes 
SELECT * FROM ETL_PROJECT_STAGING.PUBLIC.covid_19_stream WHERE SNO IN (1, 2);

--12. Create Task to transfer changes in staging table to final table automatically in the scheduled time i.e. (1 minute). By default, it will be in stopped status.Logic as:
--check fields – Reported Date and State pair
--If Reported Date and State pair is not available, then new record creation.
--If Reported Date and State pair is available, then update data.
--Use below query to prepare task.

Create or replace task task_covid_19    
WAREHOUSE  = compute_wh    
SCHEDULE  = '1 MINUTE'
WHEN  
SYSTEM$STREAM_HAS_DATA('ETL_PROJECT_STAGING.PUBLIC.covid_19_stream')
As
merge into ETL_PROJECT_FINAL.PUBLIC.COVID_19 cov_final 
using ETL_PROJECT_STAGING.PUBLIC.covid_19_stream cov_stg_stream on cov_stg_stream.REPORTEDDATE = cov_final.REPORTEDDATE 
and 
cov_stg_stream.state = cov_final.state
when matched    
then update set
cov_final.CONFIRMEDINDIANNATIONAL = cov_stg_stream.CONFIRMEDINDIANNATIONAL,
cov_final.CONFIRMEDFOREIGNNATIONAL = cov_stg_stream.CONFIRMEDFOREIGNNATIONAL,
cov_final.CURED = cov_stg_stream.CURED,
cov_final.DEATHS = cov_stg_stream.DEATHS,
cov_final.CONFIRMED = cov_stg_stream.CONFIRMED
when not matched    
then insert (REPORTEDDATE,STATE,CONFIRMEDINDIANNATIONAL,CONFIRMEDFOREIGNNATIONAL,CURED,DEATHS,CONFIRMED)    
Values(cov_stg_stream.REPORTEDDATE,cov_stg_streamcov_stg_stream.STATE,cov_stg_stream.CONFIRMEDINDIANNATIONAL,cov_stg_stream.CONFIRMEDFOREIGNNATIONAL,    cov_stg_stream.CURED,cov_stg_stream.DEATHScov_stg_stream.CONFIRMED);

--13.0.Show task
DESCRIBE TASK ETL_PROJECT_STAGING.PUBLIC.task_covid_19;//suspended by default
--13.1. Resume the task using below query
ALTER TASK IF EXISTS task_covid_19 RESUME;

--14. Again Check The final table
Select * from ETL_PROJECT_FINAL.PUBLIC.COVID_19;

--------------------Data Transformation Using PySpark on Final Table------------------------------------
--Now, we will apply some transformations to the data stored in the final table and store it again in the Snowflake Analytics database. We will utilize AWS Glue to execute PySpark jobs for this purpose. The objective is to store the data in three tables: one for monthly cases, another for quarterly cases, and a third for half-yearly cases, all organized by state for each column.
--1.Create Database 
Create database ETL_PROJECT_Analytics;

--2.Use database
Use ETL_PROJECT_Analytics;

--3.0 Create 1st table
Create or replace table monthly_covid_report(
    comonth varchar(15),
    coyear Number,
    state varchar(15),
    confirmedIndianNational Number,
    confirmedForeignNational Number,
    Cured Number,
    Deaths Number,
    Confirmed Number
)
comment = "This table consists of monthly data to track number of people getting covid belongs to india or not as well as number of deaths/cured/confirmed";

--3.1 Create 1st table
Create or replace table quarterly_covid_report(
    coquarter varchar(15),
    coyear Number,
    state varchar(15),
    confirmedIndianNational Number,
    confirmedForeignNational Number,
    Cured Number,
    Deaths Number,
    Confirmed Number
)
comment = "This table consists of quarterly data to track number of people getting covid belongs to india or not as well as number of deaths/cured/confirmed";

--Create 1st table
Create or replace table half_yearly_covid_report(
    cohalf varchar(15),
    coyear Number,
    state varchar(15),
    confirmedIndianNational Number,
    confirmedForeignNational Number,
    Cured Number,
    Deaths Number,
    Confirmed Number
)
comment = "This table consists of halfyearly data to track number of people getting covid belongs to india or not as well as number of deaths/cured/confirmed";

--4.Verify the table creation using query 
Show Tables;



---- -----------------------AWS Glue Setup for running PySpark Jobs--------------------------
--To run PySpark Jobs, we need dependent Jars Spark Snowflake Connector and Snowflake Driver. Please download the dependent jars from here. We will be making use of this jar from the S3 bucket. So, please go ahead and create an S3 bucket and upload both the Jars as prerequisite.

--1. In IAM > Roles > Create New Role > Choose Trusted Entity Type as AWS Service  and select Glue as Use Cases for other service in the bottom of the page
--2. Attach policies with full access to S3 and Glue
--3. Proceed with default actions and create role. Hence, we are done with creation of role to call other AWS Services from Glue
--4. In AWS Glue > Jobs > Create > Job Details > Provide following details
  --IAM Role – Newly Created IAM Role above
  --Glue version – Glue 3.0
  --Language – Python 3
  --Worker type – G.1X
  --Request number of workers – 2
  --Number of retries – 3
  --job timeout – 5 
--5. Click on Save. We are ready to go for scripting in script tab

-------Final output in Snowflake------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM ETL_PROJECT_Analytics.PUBLIC.half_yearly_covid_report LIMIT 10;



  






























