CREATE OR REPLACE STAGE dev.load.s3_stage URL = 's3://markazapp-data/markazapp-raw/'
  CREDENTIALS=(AWS_KEY_ID='AKIATWFTPPO7AQAROYNX' AWS_SECRET_KEY='');

CREATE OR REPLACE FILE FORMAT dev.load.markazapp_fileformat type = 'csv' field_delimiter = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' skip_header = 1;  

LIST @dev.load.s3_stage;