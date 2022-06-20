from getTimelineUtil import getTimelineWithID
import json
import os
import datetime
from os.path import exists
from util import check_s3folder_exists, create_s3folder, upload_s3, check_startdate_in_bookmark, update_bookmark
import boto3

def lambda_handler(event, context):
    # TODO implement
    
    #Local development or Cloud usage
    # If tested locally set variable local to True, if the lambda function is deploy to AWS set the variable local to False
    local=False 
    if local:
        # Set the environment Variable to: 
        environment ="dev"
    else: 
        #Get Environment Variable for production or development environment
        environment =os.environ.get("ENVIRONMENTVAL")
    #Get access to the AWS Parameter Store
    ssm = boto3.client(service_name='ssm', region_name='eu-central-1')
    
    #Get Env Variables from the AWS Parameter Store
    if environment =="dev":
        bucket=ssm.get_parameter(Name='DEV_TWEETS_DATA', WithDecryption=False)['Parameter']['Value']
        bucket_meta_data=ssm.get_parameter(Name='DEV_TWEETS_SOURCE_META', WithDecryption=False)['Parameter']['Value']
    elif environment =="prod":
        bucket=ssm.get_parameter(Name='PROD_TWEETS_DATA', WithDecryption=False)['Parameter']['Value']
        bucket_meta_data=ssm.get_parameter(Name='PROD_TWEETS_META_SOURCE', WithDecryption=False)['Parameter']['Value']
    else: 
        raise
    tweet_startdate_default=ssm.get_parameter(Name='TWEET_STARTDATE_DEFAULT', WithDecryption=False)['Parameter']['Value']   #Example '2022-06-12T08:00:00Z'
    user_ids= ssm.get_parameter(Name='UserIDs', WithDecryption=False)['Parameter']['Value']  
    user_ids=user_ids.split(",")
    max_results=100 #5 up to 100
    #Calculate the prefxes and current folder structure
    prefix=f'{environment}/timeline/{datetime.date.today().year}/{datetime.date.today().month}/{datetime.date.today().day}'
    current_folder=f'{datetime.datetime.now().hour}/{datetime.datetime.now().minute}'
    prefix_bookmark=f'{environment}/meta/tweets_batchload_bookmarks'

    #Iterate through the the list of user_ids to fetch the current timeline entries for each user id
    for user_id in user_ids:
        pagination_token=None
        next_round=True
        tweet_startdate=check_startdate_in_bookmark(bucket_meta_data, f'{prefix_bookmark}/bookmark_{user_id}',tweet_startdate_default)
        if check_s3folder_exists(bucket,f'{prefix}/',f'{current_folder}/'):
            pass
        else: 
            create_s3folder(bucket,prefix,f'{current_folder}/')
       
        while next_round:
            try:
                data=getTimelineWithID(user_id,tweet_startdate, pagination_token,max_results)
                try:
                    file_name=f'timeline_{user_id}_{data["meta"]["oldest_id"]}_{data["meta"]["newest_id"]}.json'
                    file=f'{prefix}/{current_folder}/{file_name}'
                    try: 
                        print("The next pagination_token is:",data["meta"]["next_token"])
                        pagination_token=data["meta"]["next_token"]
                        next_round=True
                    except:
                        next_round=False
                        print("No futher pagination_token available!")
                          
                    tweets=""
                    #Each entry of the list of single jsons will be written in on single line to be readable by glue crawler
                    for i in range(0,len(data['data'])):
                        data['data'][i]["ingested_at_int"]=int(datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S"))
                        line = json.dumps(data['data'][i])
                        print(line)
                        tweets= tweets + line + '\n'                            
                    upload_s3(tweets, bucket, file)              
                    update_bookmark(bucket_meta_data, f'{prefix_bookmark}/bookmark_{user_id}',f"{datetime.datetime.utcnow().isoformat()[:-7]}Z")
                except:
                    next_round=False
            except:
                raise Exception(f"Fetching the latest Tweets from {user_id} failed at {datetime.datetime.utcnow().isoformat()[:-7]}Z")
    
    #After the batch load of the timeline data is completed return the status code 200
    return {
            'statusCode': 200,
            'body': json.dumps('Tweets batch load was successful')
        }
