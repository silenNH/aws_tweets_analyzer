import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import datetime

print("Get Entities Script is starting!")

#Set Environment Variables
bucket='tweets-processed'
environment='dev'
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

#Create glueContext
glueContext = GlueContext(SparkContext.getOrCreate())

#Initialize Bookmark
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Ge tweets table
tweetsddf = glueContext.create_dynamic_frame.from_catalog(database="tweets-ingested-db", table_name="timeline", transformation_ctx = "tweetsddf")


#Relationize the table tweets
tweets_relationized=tweetsddf.relationalize("root", "s3://glue-pyspakk-test/rel_folder_for_tweets/")

#Rename the root table of tweets_relationized and change dynamic data frame to normal pyspark data frame
try:
    root=tweets_relationized.select('root')
    rootdf=root.toDF()
    rootdf=rootdf.withColumnRenamed('entities.urls',"urls_id")
    rootdf=rootdf.withColumnRenamed('entities.hashtags',"hashtags_id")
    rootdf=rootdf.withColumnRenamed('entities.mentions',"mentions_id")
    rootdf=rootdf.withColumnRenamed('entities.annotations',"annotations_id")
    rootdf=rootdf.withColumnRenamed('id',"tweet_id")
except:
    pass

#HASHTAGS
#Generate data frame for hashtags and adjust the columns
try:
    hashtags=tweets_relationized.select('root_entities.hashtags')
    hashtagsdf=hashtags.toDF()
    hashtagsdf=hashtagsdf.drop("entities.hashtags.val.start","entities.hashtags.val.end")
    hashtagsdf=hashtagsdf.withColumnRenamed('entities.hashtags.val.tag',"hashtags")
    #Join the information of tweet_id & author_id from root into hashtagsdf
    hashtags_full_df=hashtagsdf.join(rootdf, rootdf['hashtags_id'] == hashtagsdf['id'])
    hashtags_full_df=hashtags_full_df.drop("urls_id","hashtags_id","mentions_id",'annotations_id','lang','created_at','text','partition_0','partition_1','partition_2','partition_3','partition_4')
    #Save the resulting table of hashtags in S3: 
    entity_type='hashtags'
    prefix=f'{environment}/tweet_entities/{entity_type}/{datetime.date.today().year}/{datetime.date.today().month}/{datetime.date.today().day}/{datetime.datetime.now().hour}/{datetime.datetime.now().minute}'
    s3_bucket=f's3://{bucket}/{prefix}/'
    #hashtags_full_df.write.mode('ignore').json(s3_bucket)
    hashtags_full_df.coalesce(1).write.format('json').save(s3_bucket)
except:
    pass

#ANNOTATIONS: 
#Generate data frame for annotation and adjust the columns
try:
    annotations=tweets_relationized.select('root_entities.annotations')
    annotationsdf=annotations.toDF()
    annotationsdf=annotationsdf.drop("entities.annotations.val.start","entities.annotations.val.end",'entities.annotations.val.probability')
    annotationsdf=annotationsdf.withColumnRenamed('entities.annotations.val.type',"annotation_type")
    annotationsdf=annotationsdf.withColumnRenamed('entities.annotations.val.normalized_text',"normalized_text")
    #Join the information of tweet_id & author_id from root into annotation
    annotations_full_df=annotationsdf.join(rootdf, rootdf['annotations_id'] == annotationsdf['id'])
    annotations_full_df=annotations_full_df.drop("urls_id","hashtags_id","mentions_id",'annotations_id','lang','created_at','text','partition_0','partition_1','partition_2','partition_3','partition_4')
    #Save the resulting table of hashtags in S3: 
    entity_type='annotations'
    prefix=f'{environment}/tweet_entities/{entity_type}/{datetime.date.today().year}/{datetime.date.today().month}/{datetime.date.today().day}/{datetime.datetime.now().hour}/{datetime.datetime.now().minute}'
    s3_bucket=f's3://{bucket}/{prefix}/'
    #annotations_full_df.write.mode('ignore').json(s3_bucket)
    annotations_full_df.coalesce(1).write.format('json').save(s3_bucket)
except:
    pass

#Mentions: 
#Generate data frame for Mentions and adjust the columns
try:
    mentions=tweets_relationized.select('root_entities.mentions')
    mentionsdf=mentions.toDF()
    mentionsdf=mentionsdf.drop("entities.mentions.val.start","entities.mentions.val.end")
    mentionsdf=mentionsdf.withColumnRenamed('entities.mentions.val.username',"mentioned_username")
    mentionsdf=mentionsdf.withColumnRenamed('entities.mentions.val.id',"mentioned_userid")
    #Join the information of tweet_id & author_id from root into Mentions
    mentions_full_df=mentionsdf.join(rootdf, rootdf['mentions_id'] == mentionsdf['id'])
    mentions_full_df=mentions_full_df.drop("urls_id","hashtags_id","mentions_id",'annotations_id','lang','created_at','text','partition_0','partition_1','partition_2','partition_3','partition_4')
    #Save the resulting table of hashtags in S3: 
    entity_type='mentions'
    prefix=f'{environment}/tweet_entities/{entity_type}/{datetime.date.today().year}/{datetime.date.today().month}/{datetime.date.today().day}/{datetime.datetime.now().hour}/{datetime.datetime.now().minute}'
    s3_bucket=f's3://{bucket}/{prefix}/'
    #mentions_full_df.write.mode('ignore').json(s3_bucket)
    mentions_full_df.coalesce(1).write.format('json').save(s3_bucket)
except:
    pass

#URLS: 
#Generate data frame for urls and adjust the columns
try:
    urls=tweets_relationized.select('root_entities.urls')
    urlsdf=urls.toDF()
    urlsdf=urlsdf.drop("entities.urls.val.start",'entities.urls.val.end','entities.urls.val.images')
    urlsdf=urlsdf.withColumnRenamed('entities.urls.val.url',"url")
    urlsdf=urlsdf.withColumnRenamed('entities.urls.val.expanded_url',"expanded_url")
    urlsdf=urlsdf.withColumnRenamed('entities.urls.val.display_url',"display_url")
    urlsdf=urlsdf.withColumnRenamed('entities.urls.val.title',"url_title")
    urlsdf=urlsdf.withColumnRenamed('entities.urls.val.description',"url_desciption")
    urlsdf=urlsdf.withColumnRenamed('entities.urls.val.unwound_url',"unwound_url")
    urlsdf=urlsdf.withColumnRenamed('entities.urls.val.status',"url_status")
    #Join the information of tweet_id & author_id from root into urls
    urls_full_df=urlsdf.join(rootdf, rootdf['urls_id'] == urlsdf['id'])
    urls_full_df=urls_full_df.drop("urls_id","hashtags_id","mentions_id",'annotations_id','lang','created_at','text','partition_0','partition_1','partition_2','partition_3','partition_4')
    #Save the resulting table of hashtags in S3: 
    entity_type='urls'
    #prefix=f'{environment}/tweet_entities/{entity_type}/{datetime.date.today().year}/{datetime.date.today().month}/{datetime.date.today().day}/{datetime.datetime.now().hour}'
    prefix=f'{environment}/tweet_entities/{entity_type}/{datetime.date.today().year}/{datetime.date.today().month}/{datetime.date.today().day}/{datetime.datetime.now().hour}/{datetime.datetime.now().minute}'
    s3_bucket=f's3://{bucket}/{prefix}/'
    urls_full_df.write.mode('ignore').json(s3_bucket)
    urls_full_df.coalesce(1).write.format('json').save(s3_bucket)
except:
    pass

#Update Bookmark: 
job.commit()

print("The Get-Entities-Script is completed!")
