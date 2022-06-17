from getTimelineUtil import getTimelineWithID
import json
import os
import datetime
from os.path import exists
from util import check_s3folder_exists, create_s3folder, upload_s3, check_startdate_in_bookmark, update_bookmark
import boto3

def lambda_handler(event, context):
    # TODO implement
    max_results=100 #5 up to 100
    
    #Get Environment Variable
    environment = os.environ.get("ENVIRONMENTVAL")
    #Get access to the AWS Parameter Store
    ssm = boto3.client(service_name='ssm', region_name='eu-central-1')
    #Get Env Variables from the AWS Parameter Store
    if environment =="dev":
        bucket=ssm.get_parameter(Name='DEV_TWEETS_DATA', WithDecryption=False)['Parameter']['Value']
        bucket_meta_data=ssm.get_parameter(Name='DEV_TWEETS_SOURCE_META', WithDecryption=False)['Parameter']['Value']
    elif environment =="prod":
        bucket=ssm.get_parameter(Name='PROD_TWEETS_DATA', WithDecryption=False)['Parameter']['Value']
        bucket_meta_data=ssm.get_parameter(Name='PROD_TWEETS_DATA', WithDecryption=False)['Parameter']['Value']
    else: 
        raise
    tweet_startdate_default=ssm.get_parameter(Name='TWEET_STARTDATE_DEFAULT', WithDecryption=False)['Parameter']['Value']                            #Example '2022-06-12T08:00:00Z'
    user_ids= ssm.get_parameter(Name='UserIDs', WithDecryption=False)['Parameter']['Value']  # Examples: ['1233052817390284800','851431642950402048','40129171','332617373','1701930446','37065910','998503348369272832','2541212474','2767778093','281766494','2215783724','1080799090538172416','714051110','973924410771075072','3381355079']


    prefix=f'{environment}/timeline/{datetime.date.today().year}/{datetime.date.today().month}/{datetime.date.today().day}'
    current_folder=f'{datetime.datetime.now().hour}/{datetime.datetime.now().minute}'
    prefix_bookmark=f'{environment}/meta/tweets_batchload_bookmarks'
    prefix_bookmark_forGlueJob=f'{environment}/meta/Entry_Key'
    #print(datetime.datetime.utcnow().isoformat())

    #Get Last Entry_Key
    #entry_key=check_startdate_in_bookmark(bucket_meta_data, f'{prefix_bookmark_forGlueJob}/bookmark_ForGlueJob',1)

    for user_id in user_ids:
        pagination_token=None
        next_round=True
        #tweet_startdate=check_startdate_in_bookmark(bucket, f'{environment}/landing/timeline/{user_id}/bookmark/bookmark',tweet_startdate_default)
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
                    for i in range(0,len(data['data'])):
                        #print(data['data'][i])
                        #created_at_int=int(data['data'][i]["created_at"][0:4]+data['data'][i]["created_at"][5:7]+data['data'][i]["created_at"][8:10]+data['data'][i]["created_at"][11:13]+data['data'][i]["created_at"][14:16] +"00")
                        data['data'][i]["ingested_at_int"]=int(datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S"))
                        #data['data'][i]["created_at_int"]=created_at_int
                        #entry_key=entry_key+1
                        #data['data'][i]["entry_key"]=entry_key
                        line = json.dumps(data['data'][i])
                        print(line)
                        tweets= tweets + line + '\n'                            
                    upload_s3(tweets, bucket, file)              
                    update_bookmark(bucket_meta_data, f'{prefix_bookmark}/bookmark_{user_id}',f"{datetime.datetime.utcnow().isoformat()[:-7]}Z")

                except:
                    next_round=False
            except:
                raise Exception(f"Fetching the latest Tweets from {user_id} failed at {datetime.datetime.utcnow().isoformat()[:-7]}Z")
    
    #Update Bookmark for Pyspark-Glue Job with the the date of the date of the last process_run:
    #update_bookmark(bucket_meta_data, f'{prefix_bookmark_forGlueJob}/bookmark_ForGlueJob',str(entry_key))
    #print(f'The last entry_key is: {entry_key}')   
    return {
            'statusCode': 200,
            'body': json.dumps('Tweets batch load was successful')
        }
