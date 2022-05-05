#importing required modules
import botocore

#Function to test s3 connection
def testConnection(s3, bucket_name):
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        print("S3 bucket exists")
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print("S3 bucket does not exist")