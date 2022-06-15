from getTimelineUtil import getTimelineWithID
import json
import os
import datetime
from os.path import exists
from util import check_s3folder_exists, create_s3folder, upload_s3, check_startdate_in_bookmark, update_bookmark

user_ids=['40129171', '332617373', '1701930446']
max_results=100 #5 up to 100
tweet_startdate_default='2022-05-10T17:00:00Z'
environment="dev"
bucket="tweets-ingested"

for user_id in user_ids:
    pagination_token=None
    next_round=True
    tweet_startdate=check_startdate_in_bookmark(bucket, f'{environment}/landing/timeline/{user_id}/bookmark_{user_id}',tweet_startdate_default)
    prefix=f'{environment}/landing/timeline'
    if check_s3folder_exists(bucket,f'{prefix}/',f'{user_id}/'):
        pass
    else: 
        create_s3folder(bucket,prefix,f'{user_id}/')
   
   
    while next_round:
        try:
            data=getTimelineWithID(user_id,tweet_startdate, pagination_token,max_results)
            try:
                file_name=f'timeline_{user_id}_{data["meta"]["oldest_id"]}_{data["meta"]["newest_id"]}.json'
                file=f'{environment}/landing/timeline/{user_id}/{file_name}'
                try: 
                    print("The next pagination_token is:",data["meta"]["next_token"])
                    pagination_token=data["meta"]["next_token"]
                    next_round=True
                except:
                    next_round=False
                    print("No futher pagination_token available!")
                body=json.dumps(data, indent=4, sort_keys=True)
                upload_s3(body, bucket, file)
                update_bookmark("tweets-ingested", f"{environment}/landing/timeline/{user_id}/bookmark_{user_id}",f"{datetime.datetime.utcnow().isoformat()[:-7]}Z")
            except:
                next_round=False
        except:
            raise Exception(f"Fetching the latest Tweets from {user_id} failed at {datetime.datetime.utcnow().isoformat()[:-7]}Z")
    
