truncate table load.creditbook.analytics_LD;

copy into load.creditbook.analytics_LD
from @load.creditbook.s3_stage
  PATTERN ='.*analytics.csv'
  FILE_FORMAT = 'load.creditbook.creditbook_fileformat';