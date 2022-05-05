# importing required modules
from urllib import request
import boto3
import botocore
import json 
from datetime import date, timedelta, datetime
import urllib3
from testConnection import testConnection
from snowflake_connector import start_date,insert_log,call_sproc
from s3Creds import access_key_id,secret_access_key

#Creating a Boto3 session
session= boto3.Session(
    aws_access_key_id = access_key_id,
    aws_secret_access_key = secret_access_key
)
s3 = session.resource('s3')
bucket_name = 'pythonsnowflakepoc1'
bucket = s3.Bucket(bucket_name)
var = urllib3.PoolManager()

#Testing if the bucket exists in s3
testConnection(s3, bucket_name)

sdate = start_date()
edate = datetime.now() - timedelta(days=3)


#Function to dump json files to s3 bucket
def load_data(sdate,edate,var):
    sdate=sdate.strftime("%Y-%m-%d")
    edate = edate.strftime("%Y-%m-%d")
    r = var.request('GET', f'https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/date-range/{sdate}/{edate}')
    data = r.data
    object = s3.Object(bucket_name, f'fullData/data_{edate}.json')
    object.put(Body = data)
    insert_log(("data_"+edate+".json"),sdate,edate)

#function call to dump json files to s3 bucket
load_data(sdate,edate,var)

#function call to execute snowflake stored procedure
call_sproc(sdate,edate)

file = open('C:/Users/amith.thiruveedhi/Desktop/snowflake-python POC/PYTHON  CODE/dummy.txt', 'w')

file.write('hello world !')

file.close()
