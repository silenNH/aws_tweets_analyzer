import unittest
from lambda_tweets_loader.getTimelineUtil import create_url


class TestLambda(unittest.TestCase):

    def test_create_url(self):
        print('test create_url')
        self.assertEqual(create_url("NielsID","202202011000",max_results=100), 'https://api.twitter.com/2/users/NielsID/tweets?max_results=100&start_time=202202011000')
        self.assertEqual(create_url("NielsID","202202011000",pagination_token="DiesIstEinPaginationToken",max_results=100), 'https://api.twitter.com/2/users/NielsID/tweets?max_results=100&start_time=202202011000&pagination_token=DiesIstEinPaginationToken')

