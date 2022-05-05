#importing required modules
import snowflake.connector
import pandas as pd
from datetime import datetime,timedelta
import time

# Connecting to Snowflake
ctx = snowflake.connector.connect(
    user='PythonSnowFlakePOC',
    password='Murukutl@09@',
    account='nab25103.us-east-1',
    warehouse='compute_wh',
    database='POC'
    #schema='PUBLIC'
    )

#Function to get end date of last uploaded json file 
def start_date():
    cs = ctx.cursor()
    try:
        cs.execute("select COALESCE(DATEADD(DAY,1,max(BATCH_END_DATE)),'2020-01-02') from metadata.file_log")
        start_date = cs.fetchone()[0]
    finally:
        cs.close()
    return start_date

#Function to insert a log in file_log table
def insert_log(fname,sdate,edate):
    cs = ctx.cursor()
    try:
        #print(date)
        sdate = str(pd.to_datetime(sdate).date())
        edate = str(pd.to_datetime(edate).date())
        cs.execute("insert into metadata.file_log(BATCH_START_DATE,BATCH_END_DATE,FILE_NAME,STATUS,INSERT_TIME,UPDATE_TIME) values(cast('"+sdate+"'as date),cast('"+edate+"'as date),"+"'"+fname+"'"+",'success',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
    finally:
        cs.close()

#Function to call stored procedure
def call_sproc(sdate,edate):
    cs = ctx.cursor()
    try:
        sdate = str(pd.to_datetime(sdate).date())
        edate = str(pd.to_datetime(edate).date())
        time.sleep(120)
        cs.execute("call proc_date_dim("+sdate+","+edate+")")
        cs.execute("call sp_data_stream_proc()")
    finally:
        cs.close()

