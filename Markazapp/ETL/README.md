# Datawarehouse design:

https://ro32984.ap-south-1.aws.snowflakecomputing.com/oauth/authorize?client_id=CyJSPerT2e09OW7TAncKL52asXMwow%3D%3D&display=popup&redirect_uri=https%3A%2F%2Fapps-api.c1.ap-south-1.aws.app.snowflake.com%2Fcomplete-oauth%2Fsnowflake&response_type=code&scope=refresh_token&state=%7B%22browserUrl%22%3A%22https%3A%2F%2Fapp.snowflake.com%2Fonxieuu%2Fwe74620%2FwiPIcdKszUP%23query%22%2C%22csrf%22%3A%224cb4e2de%22%2C%22isSecondaryUser%22%3Afalse%2C%22oauthNonce%22%3A%2272daef34fca2f515%22%2C%22url%22%3A%22https%3A%2F%2Fro32984.ap-south-1.aws.snowflakecomputing.com%22%2C%22windowId%22%3A%226239ebb9-54ee-4b70-96a6-c2704ec5d57b%22%7D

I've used Snowflake as my cloud datawarehouse. The DWH Design is as following:

raw data files are in an s3 stage -> files from s3 are loaded to snowflake load database tables (this is exact copy of what the source files in s3 are. Its a trunc load on daily basis) 
-> the data from load database goes into stage database where it is maintained forever. This would be merge into (UPSERT) if our source files is an incremental drop of data

s3 file users.csv -> load.creditbook.users_LD -> users_LD to stage.creditbook.users

For this case, I downloaded the csv files from the links manually, and then uploaded them to my s3 account. Snowflake can directly be connected to an external s3 stage which makes it
very easy to ingest the data from s3 to snowflake.

### How the pipeline works:

- Python code to upload the file from local system to s3 bucket (s3_file_upload_code.py). The s3 keys are commented out for security purposes
-  Snowflake stage is created which is external stage and it connects to that s3 bucket/folder directly. This is done in snowflake (s3_stage.sql)
- 2 of the 3 files have a field called data, which has json data in it. The reason I opted for snowflake was because of this that snowflake deals
   with json data directly inside snowflake. We can store json data in a column with datatype of variant
- tables with _raw will have json data as it in them. This is to just load the json data as it is in json form
- then individual keys are extracted from that json and making it a structure data. Flattening the json
- the files/queries moving data from s3 to their respective load tables are transactions_load_data.sql, users_load_data.sql, analytics_load_data.sql
- the stage tables moving data from load to stage are via transactions_stage.sql, users_stage.sql, analytics_stage.sql
- the final dw table is in the file final_table.sql
- the requirements for this were little vague, so I've created a table from what I could understand, but this won't be probably what's required
- I've used Apache Airflow for orchestration of this whole process (creditbook_dag.py)
- the file schema.sql has all the schema set up commands for databases, stages, tables


### Tools Used and why:

- Snowflake : Excellent cloud based datawarehouse with 0 setup efforts required. Has excellent python connector and can leverage json format data pretty efficiently
- Airflow : Open source goes perfectly hand in hand with python. Great for scheduling and monitoring of ETL jobs
- Python : Mixture of both above. Snowflake via python is pretty straight forward, can easily run queries via python snowflake connector and normal python functions can easily be converted to airflow dags


In order to view the snowflake tables, you can via this link. Its a trial account and will expire in couple of weeks
Snowflake login url and credentials (browser based link):

snowflake url: https://ro32984.ap-south-1.aws.snowflakecomputing.com/console/login
user: admin
password: 


We can further discuss this in a meeting where I can show live demo of how airflow is working as well to complete this ETL pipeline
