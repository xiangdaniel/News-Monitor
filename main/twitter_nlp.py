"""
Twitter NLP
Written by Daniel Xiang
"""

import sys
import nltk
from nltk import pos_tag
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn
from nltk.corpus import sentiwordnet as swn
from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from load_tools import tweets_schema
import call_selenium_client
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from newspaper import Article
import uuid

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
spark = SparkSession.builder.appName('twitter nlp')\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/news.news_data") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/news.news_data")\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
nltk.data.path.append('/Users/Dao/nltk_data')
sc = spark.sparkContext
sc.addPyFile("selenium_client.py")
sc.addPyFile("call_selenium_client.py")
udf_get_comments = functions.udf(call_selenium_client.Comments(), returnType=types.ArrayType(types.ArrayType(types.StringType())))


#@functions.udf(returnType=types.ArrayType(types.StringType()))
def py_morphy(tokens):
    # print('2')
    from nltk.corpus import wordnet as wn
    nltk.data.path.append('/Users/Dao/nltk_data')
    if not isinstance(tokens, list):
        tokens = [tokens]
    modified_tokens = []
    for token in tokens:
        modified_token = wn.morphy(token)
        if modified_token is None:
            continue
        modified_tokens.append(modified_token)
    return modified_tokens


udf_morphy = functions.udf(py_morphy, returnType=types.ArrayType(types.StringType()))


def sentiment_score(comment):
    # print('3')
    s = SentimentIntensityAnalyzer()
    return s.polarity_scores(comment)['compound']


udf_sentiment_score = functions.udf(sentiment_score, returnType=types.FloatType())


def classify_tokens(tokens):
    # print('4')
    from nltk.corpus import wordnet as wn
    nltk.data.path.append('/Users/Dao/nltk_data')
    list_tokens = tokens.split()
    if not isinstance(list_tokens, list):
        list_tokens = [list_tokens]
    list_token = []
    for token in list_tokens:
        tag = wn.synsets(token)[0].pos()  # ADJ, ADJ_SAT, ADV, NOUN, VERB = 'a/JJ', 's', 'r', 'n', 'v'
        if tag == 'a' or tag == 's' or pos_tag([token])[0][1] == 'JJ' or pos_tag([token])[0][1] == 'JJS':
            list_token.append(token)
    return ' '.join(list_token)


udf_classify_tokens = functions.udf(classify_tokens, returnType=types.StringType())


def main(topic):
    # 1. Load Data, Combine keywords, tweet_urls by news_url, Add id
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', topic)\
        .option('failOnDataLoss', 'false')\
        .option('auto.offset.reset', 'earliest')\
        .load()
    values = messages.select(messages['value'].cast('string'))
    words = values.select(
        functions.explode(
            functions.split(values.value, ';')
        ).alias("words")
    )
    data = words.withColumn('text', functions.split('words', ',')).select('text')
    data = data.withColumn('news_id', data['text'][0])
    data = data.withColumn('news_keyword', data['text'][1])
    data = data.withColumn('news_url', data['text'][2])
    data = data.withColumn('tweet_url', data['text'][3])
    data = data.withColumn('retweet_count', data['text'][4])
    data = data.withColumn('favorite_count', data['text'][4])
    # data = data.dropDuplicates(['tweet_url', 'news_url'])
    data = data.withColumn('favorite_count', data['favorite_count'].cast(types.IntegerType()))
    # data = data.groupby('tweet_id', 'news_keyword', 'tweet_url', 'news_url').max('favorite_count')
        # data = data.select('news_url', 'tweet_url').distinct()
        # data = data.groupby('news_url', 'tweet_url').agg(
        #     functions.collect_set('news_keyword').alias('news_keywords')
        # )
        #functions.collect_set('tweet_url').alias('tweet_urls')

    # udf_uuid = functions.udf(lambda: str(uuid.uuid4()), returnType=types.StringType())
    # data = data.withColumn('news_id', udf_uuid())
    # data = data.select('news_id', 'news_keyword', 'favorite_count', 'news_url', 'tweet_url')
    print('finish load data')

    # 2. Scrap the news_text and tweets_comments
    data = data.withColumn('tweets_infos', udf_get_comments(data['tweet_url']))
    data = data.withColumn('tweets_info', functions.explode(data['tweets_infos']))
    data = data.select('news_id',
                       'news_keyword',
                       'retweet_count',
                       'favorite_count',
                       'news_url',
                       'tweet_url',
                       data.tweets_info[0].alias('like_counts'),
                       data.tweets_info[1].alias('comment_time'),
                       data.tweets_info[2].alias('tweets_comment'))
    data = data.withColumn('like_counts', data['like_counts'].cast(types.IntegerType()))
    # data = data.where(data['news_text'].isNotNull() & (functions.length(data['news_text']) > 0))
    data = data.where(data['tweets_comment'].isNotNull() & (functions.length(data['tweets_comment']) > 0)) # filter reviews with no text
    print('finish scrap')

    # 3. ML pipeline: Tokenization (with Regular Expression) and Remove Stop Words
    data = data.withColumn('sentiment_score', udf_sentiment_score(data['tweets_comment']))
    # news_regex_tokenizer = RegexTokenizer(inputCol='news_text', outputCol='news_words', pattern='[^A-Za-z]+')
    # news_stopwords_remover = StopWordsRemover(inputCol='news_words',
    #                                           outputCol='news_tokens',
    #                                           stopWords=StopWordsRemover.loadDefaultStopWords('english'))
    tweets_regex_tokenizer = RegexTokenizer(inputCol='tweets_comment', outputCol='tweets_words', pattern='[^A-Za-z]+')
    tweets_stopwords_remover = StopWordsRemover(inputCol='tweets_words',
                                                outputCol='tweets_tokens',
                                                stopWords=StopWordsRemover.loadDefaultStopWords('english'))
    # count_vectorizer = CountVectorizer(inputCol='filtered_words', outputCol='features')
    nlp_pipeline = Pipeline(stages=[tweets_regex_tokenizer, tweets_stopwords_remover])
    model = nlp_pipeline.fit(data)
    nlp_data = model.transform(data).select('news_id',
                                            'news_keyword',
                                            'retweet_count',
                                            'favorite_count',
                                            'news_url',
                                            'tweet_url',
                                            'tweets_comment',
                                            'tweets_tokens',
                                            'sentiment_score',
                                            'like_counts',
                                            'comment_time')

    # 4. Select Features
    # nlp_data = nlp_data.withColumn('news_tokens', udf_morphy(nlp_data['news_tokens']))
    nlp_data = nlp_data.withColumn('tweets_tokens', udf_morphy(nlp_data['tweets_tokens']))
    # nlp_data = nlp_data.select(nlp_data['business_id'], review['stars'], udf_morphy(review['tokens']).alias('tokens'))
    # nlp_data = nlp_data.where(functions.size(nlp_data['news_tokens']) > 0)
    nlp_data = nlp_data.where(functions.size(nlp_data['tweets_tokens']) > 0)

    # 5. Calculate Weighted Sentiment Scores
    # nlp_data = nlp_data.withColumn('sentiment_score', udf_sentiment_score(nlp_data['tweets_tokens']))
    nlp_data = nlp_data.withColumn('tweets_tokens', functions.concat_ws(' ', 'tweets_tokens'))
    # nlp_data = nlp_data.withColumn('classify_tokens', udf_classify_tokens(review['tokens']))
    nlp_data_score = nlp_data.groupby('news_id', 'news_keyword', 'retweet_count', 'favorite_count', 'news_url', 'tweet_url').agg(
        functions.collect_list('tweets_tokens').alias('tweets_tokens'),
        functions.collect_list('sentiment_score').alias('sentiment_scores'),
        functions.collect_list('like_counts').alias('like_counts'),
        functions.collect_list('comment_time').alias('comment_time'),
        (functions.sum(nlp_data.sentiment_score * nlp_data.like_counts) / functions.sum(nlp_data.like_counts)).alias(
            'weighted_sentiment_score')
    )
    nlp_data_score = nlp_data_score.withColumn('tweets_tokens', functions.concat_ws(' ', 'tweets_tokens'))
    nlp_data_score = nlp_data_score.withColumn('tweets_tokens', udf_classify_tokens(nlp_data_score['tweets_tokens']))
    # nlp_data_score = nlp_data_score.withColumn('tweets_tokens', functions.split('tweets_tokens', '\s+'))
    # nlp_data_score = nlp_data_score.withColumn('news_tokens', functions.concat_ws(' ', 'news_tokens'))
    nlp_data_score = nlp_data_score.withColumn('comment_time', functions.concat_ws(',', 'comment_time'))
    print('finish scores')

    # 6. Save

    # nlp_data_score.write.format("com.mongodb.spark.sql.DefaultSource")\
    #     .mode("append")\
    #     .option("uri", "mongodb://127.0.0.1:27017/news.news_data")\
    #     .option("replaceDocument", False)\
    #     .option("database", "news")\
    #     .option("collection", "news_data").save()

    nlp_data_score = nlp_data_score.withColumn(
        'dl_value', functions.to_json(functions.struct(
            [nlp_data_score[x] for x in nlp_data_score.columns])
        )
    )
    stream = nlp_data_score.select(nlp_data_score.news_id.alias("key"),
                                   nlp_data_score.dl_value.alias("value"))\
        .writeStream\
        .format('kafka')\
        .outputMode('complete')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option("topic", "nlp-2")\
        .option("checkpointLocation", "../check")\
        .start()

    # stream = nlp_data_score.writeStream.format('console').outputMode('complete').start()
    # stream = nlp_data_score.writeStream\
    #     .format('json')\
    #     .outputMode('update')\
    #     .option("path", "/Users/Dao/Documents/BigData/733/project/twitter/streaming/data")\
    #     .option("checkpointLocation", "../check")\
    #     .start()
    stream.awaitTermination()


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
