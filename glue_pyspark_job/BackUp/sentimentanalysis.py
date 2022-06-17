import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
glueContext = GlueContext(SparkContext.getOrCreate())


from pyspark.sql.types import StringType
comprehend = boto3.client(service_name='comprehend', region_name='eu-central-1')

def get_sentiment(text, lang):
    if lang !="en":
        return "Wrong Language"
    else:
        return comprehend.detect_sentiment(Text=text, LanguageCode=lang)['SentimentScore']
    
spark.udf.register(name='get_sentiment', f=get_sentiment, returnType=StringType())
tweetsddf = glueContext.create_dynamic_frame.from_catalog(database="tweets-ingested-db", table_name="timeline")
tweets_sentiment=tweetsddf.toDF()
tweets_sentiment=tweets_sentiment.filter("lang = 'en'")
tweets_sentiment=tweets_sentiment.drop("partition_0","partition_1","partition_2","partition_3","partition_4","entities","ingested_at_int")
tweets_sentiment=tweets_sentiment.limit(10)
tweets_sentiment=tweets_sentiment.withColumn("sentiment", get_sentiment(col("text"),col("lang")))



Jupyter Notebook
Untitled
Last Checkpoint: vor ein paar Sekunden
(autosaved)
Sparkmagic (PySpark) 
File
Edit
View
Insert
Cell
Kernel
Widgets
Help

Code
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
import pyspark.sql.functions as F
glueContext = GlueContext(SparkContext.getOrCreate())
#Ge tweets table
tweetsddf = glueContext.create_dynamic_frame.from_catalog(database="tweets-ingested-db", table_name="timeline", transformation_ctx = "tweetsddf")
​
​
tweetsddf.printSchema()
AWS_REGION = 'eu-central-1'
MIN_SENTENCE_LENGTH_IN_CHARS = 10
MAX_SENTENCE_LENGTH_IN_CHARS = 4500
COMPREHEND_BATCH_SIZE = 25  ## This batch size results in groups no larger than 25 items
NUMBER_OF_BATCHES = 10
ROW_LIMIT = 10000
from pyspark.sql import Row
import boto3
SentimentRow = Row("review_id", "sentiment")
def getBatchSentiment(input_list):
  ## You can import the ratelimit module if you want to further rate limit API calls to Comprehend
  ## https://pypi.org/project/ratelimit/
  #from ratelimit import rate_limited
  arr = []
  bodies = [i[1] for i in input_list]
  client = boto3.client('comprehend',region_name = AWS_REGION)

  #@rate_limited(1) 
  def callApi(text_list):
    response = client.batch_detect_sentiment(TextList = text_list, LanguageCode = 'en')
    return response
  
  for i in range(NUMBER_OF_BATCHES-1):
    text_list = bodies[COMPREHEND_BATCH_SIZE * i : COMPREHEND_BATCH_SIZE * (i+1)]
    response = callApi(text_list)
    for r in response['ResultList']:
      idx = COMPREHEND_BATCH_SIZE * i + r['Index']
      arr.append(SentimentRow(input_list[idx][0], r['Sentiment']))
  
  return arr
from pyspark.sql import Row
import boto3
SentimentRow = Row("review_id", "sentiment")
def getBatchSentiment(input_list):
  ## You can import the ratelimit module if you want to further rate limit API calls to Comprehend
  ## https://pypi.org/project/ratelimit/
  #from ratelimit import rate_limited
  arr = []
  bodies = [i[1] for i in input_list]
  client = boto3.client('comprehend',region_name = AWS_REGION)
​
  #@rate_limited(1) 
  def callApi(text_list):
    response = client.batch_detect_sentiment(TextList = text_list, LanguageCode = 'en')
    return response
  
  for i in range(NUMBER_OF_BATCHES-1):
    text_list = bodies[COMPREHEND_BATCH_SIZE * i : COMPREHEND_BATCH_SIZE * (i+1)]
    response = callApi(text_list)
    for r in response['ResultList']:
      idx = COMPREHEND_BATCH_SIZE * i + r['Index']
      arr.append(SentimentRow(input_list[idx][0], r['Sentiment']))
  
  return arr
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.functions import col
import pyspark.sql.functions as F
tweetsddf = glueContext.create_dynamic_frame.from_catalog(database="tweets-ingested-db", table_name="timeline")
tweets_sentiment=tweetsddf.toDF()
tweets_sentiment=tweets_sentiment.drop("partition_0","partition_1","partition_2","partition_3","partition_4","entities","ingested_at_int")
tweets_sentiment=tweets_sentiment.withColumn('text_len', F.length(F.col('text')))
print(tweets_sentiment.count())
tweets_sentiment=tweets_sentiment.filter("lang = 'en'").filter(F.col('text_len') > MIN_SENTENCE_LENGTH_IN_CHARS).filter(F.col('text_len') < MAX_SENTENCE_LENGTH_IN_CHARS)
print(tweets_sentiment.count())
"""
filtered_tweetsddf = tweetsddf \
  .filter("lang = 'en'") \
  .withColumn('text_len', F.length('text')) \
  .filter(F.col('body_len') > MIN_SENTENCE_LENGTH_IN_CHARS) \
  .filter(F.col('body_len') < MAX_SENTENCE_LENGTH_IN_CHARS) \
  .limit(ROW_LIMIT)
"""
tweets_sentiment.printSchema()
tweets_sentiment.show()
import math
record_count = tweets_sentiment.count()
print(record_count)
print((NUMBER_OF_BATCHES * COMPREHEND_BATCH_SIZE))
number_partitions=math.ceil(record_count / (NUMBER_OF_BATCHES * COMPREHEND_BATCH_SIZE))
print(record_count / (NUMBER_OF_BATCHES * COMPREHEND_BATCH_SIZE))
#df2 = tweets_sentiment.repartition(record_count / (NUMBER_OF_BATCHES * COMPREHEND_BATCH_SIZE))
tweets_sentiment_repart = tweets_sentiment.repartition(number_partitions)
group_number_partitions=tweets_sentiment_repart.rdd.map(lambda l: (l.id, l.text)).glom()
print(group_number_partitions.take(1))
record_count.show()
sentiment = group_number_partitions.coalesce(10).map(lambda l: getBatchSentiment(l)).flatMap(lambda x: x).toDF().repartition('review_id').cache()
group_rdd = df2.rdd.map(lambda l: (l.review_id, l.review_body)).glom()
​
tweetsddf = glueContext.create_dynamic_frame.from_catalog(database="tweets-ingested-db", table_name="timeline")
tweets_sentiment=tweetsddf.toDF()
tweets_sentiment=tweets_sentiment.drop("partition_0","partition_1","partition_2","partition_3","partition_4","entities","ingested_at_int")
tweets_sentiment=tweets_sentiment.filter("lang = 'en'")
tweets_sentiment=tweets_sentiment.limit(10)
tweets_sentiment=tweets_sentiment.withColumn('Sentiment', F.length(F.col('text')))
print(tweets_sentiment.count())
tweets_sentiment=tweets_sentiment.filter("lang = 'en'").filter(F.col('text_len') > MIN_SENTENCE_LENGTH_IN_CHARS).filter(F.col('text_len') < MAX_SENTENCE_LENGTH_IN_CHARS)
print(tweets_sentiment.count())
tweets_sentiment=tweets_sentiment.limit(10)
tweets_sentiment.count()
tweets_sentiment.select("text").collect()[1]
comprehend = boto3.client(service_name='comprehend', region_name='eu-central-1')
comprehend = boto3.client(service_name='comprehend', region_name='eu-central-1')
# Run sentiment analysis
sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode='en')
# Output
sentiment_output
​
​
​
​
from pyspark.sql.types import StringType
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
glueContext = GlueContext(SparkContext.getOrCreate())
from pyspark.sql.types import StringType
comprehend = boto3.client(service_name='comprehend', region_name='eu-central-1')
​
def get_sentiment(text):
    lang="en"
    if lang !="en":
        return "Wrong Language"
    else:
        print(comprehend.detect_sentiment(Text=text, LanguageCode=lang))
        return comprehend.detect_sentiment(Text=text, LanguageCode=lang)
def returnWord(text):
    return text
​
=udf(returnWord, StringType())
get_sentiment_udf=udf(get_sentiment, StringType())
returnWord_udf=udf(returnWord, StringType())
tweetsddf = glueContext.create_dynamic_frame.from_catalog(database="tweets-ingested-db", table_name="timeline")
tweets_sentiment=tweetsddf.toDF()
tweets_sentiment=tweets_sentiment.filter("lang = 'en'")
tweets_sentiment=tweets_sentiment.drop("partition_0","partition_1","partition_2","partition_3","partition_4","entities","ingested_at_int")
tweets_sentiment=tweets_sentiment.limit(20)
tweetsddf = glueContext.create_dynamic_frame.from_catalog(database="tweets-ingested-db", table_name="timeline")
tweets_sentiment=tweetsddf.toDF()
tweets_sentiment=tweets_sentiment.filter("lang = 'en'")
tweets_sentiment=tweets_sentiment.drop("partition_0","partition_1","partition_2","partition_3","partition_4","entities","ingested_at_int")
tweets_sentiment=tweets_sentiment.limit(20)
#tweets_sentiment=tweets_sentiment.withColumn("sentiment", get_sentiment(col("text")))
tweets_sentiment
liste=tweets_sentiment.select('text').collect()
tweets_sentiment_array
tweets_sentiment_array = [int(row.mvv) for row in tweets_sentiment.collect()]
​
import pandas
sentiment_lsit = list(tweets_sentiment.select('text').toPandas()['text'])
print(sentiment_lsit[0])
sentiment_batch = comprehend.batch_detect_sentiment(TextList=sentiment_lsit,LanguageCode='en')
["Sentiment"]
sentiment_batch['ResultList'][0]["Sentiment"]
Sentiment_liste = [item.get('Sentiment') for item in sentiment_batch['ResultList']]
Sentiment_liste = [item.get('Sentiment') for item in sentiment_batch['ResultList']]
Sentiment_liste
​
​
​
​
​
​
comprehend = boto3.client(service_name='comprehend', region_name='us-east-2')
# Run sentiment analysis
sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode='en')
# Output
sentiment_output
0
tweets_sentiment.collect()[0][2]
tweets_sentiment.printSchema()
def get_sentiment(text,lang):
    # Run sentiment analysis
    comprehend = boto3.client(service_name='comprehend', region_name='eu-central-1')
    if lang =="en":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="en")["Sentiment"]
        return sentiment_output
    elif lang =="de":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="de")["Sentiment"]
        return sentiment_output
    elif lang =="pl":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="pl")["Sentiment"]
        return sentiment_output
    elif lang =="fr":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="fr")["Sentiment"]
        return sentiment_output
    elif lang =="es":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="es")["Sentiment"]
        return sentiment_output
    elif lang =="es":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="es")["Sentiment"]
        return sentiment_output
    elif lang =="ar":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="ar")["Sentiment"]
        return sentiment_output
    else:
        return "Language not supported"
get_sentiment_udf=udf(get_sentiment, StringType())
#[ar, hi, ko, zh-TW, ja, zh, de, pt, en, it, fr, es]
​
def get_sentiment(text,lang):
    # Run sentiment analysis
    comprehend = boto3.client(service_name='comprehend', region_name='eu-central-1')
    if lang =="en":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="en")["Sentiment"]
        return sentiment_output
    elif lang =="de":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="de")["Sentiment"]
        return sentiment_output
    elif lang =="pl":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="pl")["Sentiment"]
        return sentiment_output
    elif lang =="fr":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="fr")["Sentiment"]
        return sentiment_output
    elif lang =="es":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="es")["Sentiment"]
        return sentiment_output
    elif lang =="es":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="es")["Sentiment"]
        return sentiment_output
    elif lang =="ar":
        sentiment_output = comprehend.detect_sentiment(Text=text, LanguageCode="ar")["Sentiment"]
        return sentiment_output
    else:
        return "Language not supported"
get_sentiment_udf=udf(get_sentiment, StringType())
tweets_sentiment.show()
comprehend = boto3.client(service_name='comprehend', region_name='eu-central-1')
​
tweetsddf = glueContext.create_dynamic_frame.from_catalog(database="tweets-ingested-db", table_name="timeline")
tweets_sentiment=tweetsddf.toDF()
#tweets_sentiment=tweets_sentiment.filter("lang = 'en'")
tweets_sentiment=tweets_sentiment.drop("partition_0","partition_1","partition_2","partition_3","partition_4","entities","ingested_at_int")
tweets_sentiment=tweets_sentiment.limit(100)
ithColumn("sentiment", print_text_udf(F.col("text"),F.col("lang")
tweets_sentiment=tweets_sentiment.withColumn("sentiment", print_text_udf(F.col("text"),F.col("lang")))
tweets_sentiment.printSchema()
comprehend.detect_sentiment(Text="Hello", LanguageCode='en')
from pyspark.sql.functions import *
tweets_sentiment.select(col("created_at"),to_date(col("created_at"),"yyyy-MM-dd-Thh:mm:ss").alias("date")).show()
tweets_sentiment
tweets_sentiment.select(to_timestamp(tweets_sentiment.created_at).alias('to_timestamp')).show()
tweets_sentiment=tweets_sentiment.withColumn("created_at_new",to_timestamp("created_at"))
tweets_sentiment=tweets_sentiment.withColumn("created_at_new",to_timestamp("created_at"))
tweets_sentiment
tweets_sentiment.show()
sentiment.json
s3_bucket="s3://tweets-processed/dev/tweet_entities/sentiment/sentiment.json"
tweets_sentiment.coalesce(1).write.format('json').save(s3_bucket)
​
