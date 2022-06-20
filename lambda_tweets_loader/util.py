import requests, boto3, os
from botocore.errorfactory import ClientError
import datetime

#Get the Boto3 S3 client
def get_client():
    return boto3.client('s3')

#Check the satus of the bookmark for each user id. If there is no bookmark the default value tweet_startdate is used as start date
def check_startdate_in_bookmark(bucket,key,tweet_startdate):
    
    # Check wheter variable bucket exists
    if bucket == None:
        raise ValueError("The function check_startdate_in_bookmark expects the input variable bucket. But none were given")

    # Check wheter variable key exists
    if key == None:
        raise ValueError("The function check_startdate_in_bookmark expects the input variable key. But none were given")

    # Check wheter variable key exists
    if tweet_startdate == None:
        raise ValueError("The function check_startdate_in_bookmark expects the input variable tweet_startdate. But none were given")

    #Get S3 Boto3 Client 
    s3_client=get_client()
    #Try to get the bookmark in the parameter specified meta S3 Booked 
    try: 
        bookmark_file=s3_client.get_object(
            Bucket=bucket,
            Key= key)
        prev_file=  bookmark_file['Body'].read().decode('utf-8')
        print(prev_file)
    except ClientError as e: 
        if e.response['Error']['Code'] == "NoSuchKey":
            prev_file = tweet_startdate
            print(prev_file)
        else:
            print(f"An error occured by trying to fetch the existing bookmarkfor the path:{key}") 
            raise
    return prev_file

#  Update the bookmark for each user id to avoid double loads of the same tweets
def update_bookmark(bucket, key, utc_timestamp):
    # Check wheter variable bucket exists
    if bucket == None:
        raise ValueError("The function update_bookmark expects the input variable bucket. But none were given")

    # Check wheter variable key exists
    if key == None:
        raise ValueError("The function update_bookmark expects the input variable key. But none were given")

    # Check wheter variable key exists
    if utc_timestamp == None:
        raise ValueError("The function update_bookmark expects the input variable utc_timestamp. But none were given")

    #Get the S3 Boto3 client
    s3_client=get_client()
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=utc_timestamp.encode('utf-8')
            )
        return (f'Bookmark in folder {key} is set: {utc_timestamp}')
    except:
        raise Exception(f"Function update_bookmark failed! At the path:{bucket}/{key}")
    
def check_s3folder_exists(bucket,prefix,folder):
    # Check wheter variable bucket exists
    if bucket == None:
        raise ValueError("The function check_s3folder_exists expects the input variable bucket. But none were given")

    # Check wheter variable key exists
    if prefix == None:
        raise ValueError("The function check_s3folder_exists expects the input variable prefix. But none were given")

    # Check wheter variable key exists
    if folder == None:
        raise ValueError("The function check_s3folder_exists expects the input variable folder. But none were given")

    exists= False
    s3_client=get_client()
    try:
        list_objects= s3_client.list_objects(Bucket=bucket, Prefix=prefix) 
        for i in range(len(list_objects["Contents"])):
            if list_objects["Contents"][i]["Key"] == f'{prefix}{folder}':
                exists = True
    except:
        pass
    return exists
    
#If no folder exists for JSON Timeline file or for the bookmark folder, the following function will create a folder
def create_s3folder(bucket,prefix,foldername):
    # Check wheter variable bucket exists
    if bucket == None:
        raise ValueError("The function create_s3folder expects the input variable bucket. But none were given")

    # Check wheter variable key exists
    if prefix == None:
        raise ValueError("The function create_s3folder expects the input variable prefix. But none were given")

    # Check wheter variable key exists
    if foldername == None:
        raise ValueError("The function create_s3folder expects the input variable foldername. But none were given")

    #Get the S3 Boto client 
    s3_client=get_client()
    #Create the S3 Folder 
    response=s3_client.put_object(Bucket=bucket, Key=f'{prefix}/{foldername}')
    #return the response 
    return response
    
def upload_s3(body, bucket, file):
    # Check wheter variable bucket exists
    if body == None:
        raise ValueError("The function upload_s3 expects the input variable body. But none were given")

    # Check wheter variable key exists
    if bucket == None:
        raise ValueError("The function upload_s3 expects the input variable bucket. But none were given")

    # Check wheter variable key exists
    if file == None:
        raise ValueError("The function upload_s3 expects the input variable file. But none were given")
    
    #Get the S3 Boto client
    s3_client=get_client()
    #Upload file to S3 Bucket
    res=s3_client.put_object(
        Bucket=bucket,
        Key=file,
        Body=body)
    return res

