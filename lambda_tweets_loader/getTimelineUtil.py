# Template for User Timeline fetch: https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/User-Tweet-Timeline/user_tweets.py
import requests
import os
import json

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token =os.environ.get("BEARERTOKEN")

def create_url(user_id,start_time, pagination_token=None, max_results=None ):
    # Check wheter parameter max_results is in the right value range
    if max_results == None or max_results < 5 or max_results>100:
        raise ValueError("Wrong value for parameter max_reslults. Values can be in the range 5 - 100")
    
    # Check wheter parameter user_id is not none
    if user_id == None:
        raise ValueError("Parameter user_id cannot be none")
    
    # Check wheter parameter start_time is not none
    if start_time == None:
        raise ValueError("Parameter user_id cannot be none")
    
    # Check whether pagination token is given
    if pagination_token != None and max_results != None:
        return f'https://api.twitter.com/2/users/{user_id}/tweets?max_results={max_results}&start_time={start_time}&pagination_token={pagination_token}'
    elif pagination_token == None and max_results != None:
        return f'https://api.twitter.com/2/users/{user_id}/tweets?max_results={max_results}&start_time={start_time}'
    else: 
        raise Exception("the function create_url is defect!")
    
# Define the field parameter. In our case created_at, lang (language), author_id and entities (hasthags, annotations, mentions, urls, images)
def get_params():
    return {"tweet.fields": "created_at,lang,author_id,entities"}

#Create the authentification header for the REST API request
def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r

#Connection to the endpoint
def connect_to_endpoint(url, params):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

#Get the timeline of a specific user
def getTimelineWithID(user_id,start_time, pagination_token=None, max_results=None):
    url = create_url(user_id,start_time, pagination_token, max_results)
    params = get_params()
    json_response = connect_to_endpoint(url, params)
    return json_response

