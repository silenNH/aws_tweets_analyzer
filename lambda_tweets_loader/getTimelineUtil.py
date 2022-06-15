import requests
import os
import json

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARERTOKEN")

def create_url(user_id,start_time, pagination_token=None, max_results=None ):
    # Replace with user ID below
    #user_id = 1390987827790372864
    #print(max_results)
    #print(pagination_token)
    if pagination_token ==None and max_results == None:
        #print("https://api.twitter.com/2/users/{}/tweets".format(user_id))
        return "https://api.twitter.com/2/users/{}/tweets".format(user_id)
    elif pagination_token != None and max_results != None:
        #print(f'https://api.twitter.com/2/users/{user_id}/tweets?max_results={max_results}&start_time={start_time}&pagination_token={pagination_token}')
        return f'https://api.twitter.com/2/users/{user_id}/tweets?max_results={max_results}&start_time={start_time}&pagination_token={pagination_token}'
    elif pagination_token == None and max_results != None:
        #print(f'https://api.twitter.com/2/users/{user_id}/tweets?max_results={max_results}&start_time={start_time}')
        return f'https://api.twitter.com/2/users/{user_id}/tweets?max_results={max_results}&start_time={start_time}'
    elif pagination_token != None and max_results == None:
        #print(f'https://api.twitter.com/2/users/{user_id}/tweets?start_time={start_time}&pagination_token={pagination_token}')
        return f'https://api.twitter.com/2/users/{user_id}/tweets?start_time={start_time}&pagination_token={pagination_token}'
    else: 
        raise Exception("the function create_url is defect!")
    
def get_params():
    return {"tweet.fields": "created_at,lang,author_id,entities"}

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r


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


def getTimelineWithID(user_id,start_time, pagination_token=None, max_results=None):
    #print(max_results)
    url = create_url(user_id,start_time, pagination_token, max_results)
    params = get_params()
    #print(params)
    json_response = connect_to_endpoint(url, params)
    #print(json.dumps(json_response, indent=4, sort_keys=True))
    return json_response

