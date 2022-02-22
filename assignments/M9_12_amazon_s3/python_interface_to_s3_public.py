# -*- coding: utf-8 -*-
"""
WORKING WITH AN AMAZON S3 BUCKET: FILE DOWNLOAD AND UPLOAD 
Created on Tue Sep 28 13:37:34 2021
@author: apt4c

INSTRUCTIONS: 
This code will work if you:
1. supply working S3 credentials
2. update the file/bucket paths
"""

import boto3
# Boto3 is the Amazon Web Services (AWS) Software Development Kit (SDK) 
# for Python, which allows Python developers to write software that makes 
# use of services like Amazon S3 and Amazon EC2. 

import os
import pandas as pd

# Create S3 instance, passing credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id='YOUR_ACCESS_KEY_ID',
    aws_secret_access_key='YOUR_SECRET_ACCESS_KEY'
)

save_path = 'C:/Users/apt4c/Documents/work/uva/teaching/ds5110_big_data_systems/fall2021/assignments/M9_12_amazon_s3'
bucket_name = 'web-host-test1'
file_name = 'index.html'
file_fullpath = os.path.join(save_path, file_name)

# download file from S3 bucket
s3_client.download_file(bucket_name, file_name, file_fullpath)

# create pandas df and save to csv
df = pd.DataFrame([1,2,3])
panda_file_name = 'small_panda.csv'
panda_fullpath = os.path.join(save_path, panda_file_name)
df.to_csv(panda_fullpath)

# upload file to S3 bucket
s3_client.upload_file(panda_fullpath, bucket_name, panda_file_name)
