copy into load.markazapp.transactions
from @load.markazapp.s3_stage
  PATTERN ='.*transactions.csv'
  FILE_FORMAT = 'load.markazapp.markazapp_fileformat'