import os
import requests
import json
import boto3
from datetime import datetime, timedelta, date


data_inb_dir = 'C:/Users/Hp/Desktop/work/Markazapp/data'

ACCESS_KEY = os.environ['S3_ACCESS_KEY']
SECRET_KEY = os.environ['S3_SECRET_KEY']

current_date = str(date.today())
bucket = "markazapp-data"
folder = "markazapp-raw/"+current_date+'/'

def upload_files_to_s3(bucket, folder, inbound):



    s3_client = boto3.client('s3', region_name='us-east-1', aws_access_key_id=ACCESS_KEY,
                               aws_secret_access_key=SECRET_KEY)

    

    for file in os.listdir(inbound):

        
        file_to_upload = inbound+'/'+file
        print(file_to_upload)
        key = folder + file
        
        try:
            s3_client.upload_file(file_to_upload, bucket, key)
            print('Upload of file ',file,' Successful')

        except Exception as e:
            print(e)
        
    
upload_files_to_s3(bucket, folder, data_inb_dir)

#print(os.listdir(data_inb_dir))
