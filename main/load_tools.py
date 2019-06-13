from pyspark.sql import SparkSession, functions, types

# tweets_schema = types.StructType([
#     types.StructField('news_url', types.StringType(), False),
#     types.StructField('tweet_url', types.StringType(), False),
#     types.StructField('news_keyword', types.StringType(), False)
# ])

tweets_schema = types.StructType([
    types.StructField('news_id', types.StringType(), False),
    types.StructField('news_keyword', types.StringType(), False),
    types.StructField('news_url', types.StringType(), False),
    types.StructField('tweet_url', types.StringType(), False),
    types.StructField('retweet_count', types.StringType(), False),
    types.StructField('favorite_count', types.StringType(), False)
])
