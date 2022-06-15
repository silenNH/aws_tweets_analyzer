import requests, boto3, os
from botocore.errorfactory import ClientError
import datetime

tweet_startdate='2022-05-10T17:00:00Z'
environment="dev"

def get_client():
    return boto3.client('s3')


def check_startdate_in_bookmark(bucket,key,tweet_startdate):
    s3_client=get_client()
    try: 
        bookmark_file=s3_client.get_object(
            Bucket=bucket,
            Key= key) #f'{environment}/landing/bookmark')
        prev_file=  bookmark_file['Body'].read().decode('utf-8')
        print(prev_file)
    except ClientError as e: 
        if e.response['Error']['Code'] == "NoSuchKey":
            prev_file = tweet_startdate
            print(prev_file)
        else: 
            raise
    return prev_file
    
def update_bookmark(bucket, key, utc_timestamp):
    s3_client=get_client()
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=utc_timestamp.encode('utf-8')
            )
        return (f'Bookmark in folder {key} is set: {utc_timestamp}')
    except:
        raise Exception("Function update_bookmark failed!")
    
def check_s3folder_exists(bucket,prefix,folder):
    exists= False
    s3_client=get_client()
    try:
        list_objects= s3_client.list_objects(Bucket=bucket, Prefix=prefix) # prefix: f'{environment}/landing/'
        for i in range(len(list_objects["Contents"])):
            #print(list_objects["Contents"][i]["Key"])
            if list_objects["Contents"][i]["Key"] == f'{prefix}{folder}':
                exists = True
    except:
        pass #raise Exception("check_s3folder_exists function has an error")
    return exists
    

def create_s3folder(bucket,prefix,foldername):
    s3_client=get_client()
    response=s3_client.put_object(Bucket=bucket, Key=f'{prefix}/{foldername}')
    return response
    
def upload_s3(body, bucket, file):
    s3_client=get_client()
    res=s3_client.put_object(
        Bucket=bucket,
        Key=file,
        Body=body)
    return res

