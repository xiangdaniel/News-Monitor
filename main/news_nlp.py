"""
News NLP
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
spark = SparkSession.builder.appName('news nlp').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
nltk.data.path.append('/Users/Dao/nltk_data')
sc = spark.sparkContext


def get_news_info(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
    # print(article.authors)
    # article.nlp()
    # print(article.keywords)
        return article.title, article.text, article.top_image
    except Exception as e:
        print(e)
    return "", "", ""


udf_get_news_info = functions.udf(get_news_info, returnType=types.ArrayType(types.StringType()))


#@functions.udf(returnType=types.ArrayType(types.StringType()))
def py_morphy(tokens):
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


def sentiment_score(text):
    list_text = text.split('.')
    s = SentimentIntensityAnalyzer()
    list_scores = []
    for sentence in list_text:
        list_scores.append(s.polarity_scores(sentence)['compound'])
    return list_scores


udf_sentiment_score = functions.udf(sentiment_score, returnType=types.ArrayType(types.FloatType()))


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
    data = data.withColumn('news_url', data['text'][1])
    print('finish load data')

    # 2. Scrap the news_text and tweets_comments
    data = data.withColumn('news_info', udf_get_news_info(data['news_url']))
    data = data.withColumn('news_title', data['news_info'][0])
    data = data.withColumn('news_text', data['news_info'][1])
    data = data.withColumn('news_image', data['news_info'][2])
    data = data.where(data['news_title'].isNotNull() & (functions.length(data['news_title']) > 0))
    data = data.where(data['news_text'].isNotNull() & (functions.length(data['news_text']) > 0))
    # data = data.where(data['tweets_comment'].isNotNull() & (functions.length(data['tweets_comment']) > 0)) # filter reviews with no text
    print('finish scrap')

    # 3. ML pipeline: Tokenization (with Regular Expression) and Remove Stop Words
    data = data.withColumn('sentiment_scores', udf_sentiment_score(data['news_text']))
    news_regex_tokenizer = RegexTokenizer(inputCol='news_text', outputCol='news_words', pattern='[^A-Za-z]+')
    news_stopwords_remover = StopWordsRemover(inputCol='news_words',
                                              outputCol='news_tokens',
                                              stopWords=StopWordsRemover.loadDefaultStopWords('english'))
    # count_vectorizer = CountVectorizer(inputCol='filtered_words', outputCol='features')
    nlp_pipeline = Pipeline(stages=[news_regex_tokenizer, news_stopwords_remover])
    model = nlp_pipeline.fit(data)
    nlp_data = model.transform(data).select('news_id', 'news_title', 'news_text', 'news_image', 'news_tokens', 'sentiment_scores')

    # 4. Select Features
    nlp_data = nlp_data.withColumn('news_tokens', udf_morphy(nlp_data['news_tokens']))
    # nlp_data = nlp_data.withColumn('tweets_tokens', udf_morphy(nlp_data['tweets_tokens']))
    # nlp_data = nlp_data.select(nlp_data['business_id'], review['stars'], udf_morphy(review['tokens']).alias('tokens'))
    nlp_data = nlp_data.where(functions.size(nlp_data['news_tokens']) > 0)
    # nlp_data = nlp_data.where(functions.size(nlp_data['tweets_tokens']) > 0)
    # nlp_data_score = nlp_data_score.withColumn('tweets_tokens', functions.split('tweets_tokens', '\s+'))
    nlp_data = nlp_data.withColumn('news_tokens', functions.concat_ws(' ', 'news_tokens'))
    print('finish scores')

    # 5. Save
    nlp_data = nlp_data.withColumn(
        'dl_value', functions.to_json(functions.struct(
            [nlp_data[x] for x in nlp_data.columns])
        )
    )

    stream = nlp_data.select(nlp_data.news_id.alias("key"),
                             nlp_data.dl_value.alias("value"))\
        .writeStream\
        .format('kafka')\
        .outputMode('update')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option("topic", "mlnews-2")\
        .option("checkpointLocation", "../check")\
        .start()

    # stream = nlp_data.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination()


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
