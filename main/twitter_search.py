import os
import time
from datetime import datetime
import csv
from TwitterSearch import *
import pandas as pd
from uuid import uuid4


def exclude_url(url):
    exclude = ['twitter', 'youtube.', 'youtu.be', 'goo.gl', '.tv', 'tv.', 'itunes.', 'itun.es', 'apple.co', 'open.spotify', 'hyperurl.co', 'amazon.', 'amzn.', 'smarturl.']
    return url is None or any(e in url for e in exclude)


def todayString():
    return datetime.today().strftime('%Y-%m-%d')


def get_tweet_info(tweet):
    created_at = tweet['created_at']
    date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
    day = date.date().strftime('%Y-%m-%d')
    hour = date.hour
    retweet_count = tweet['retweet_count']
    favorite_count = tweet['favorite_count']
    text = tweet['text']
    tweet_url = 'https://twitter.com/' + tweet['user']['screen_name'] + '/status/' + tweet['id_str']
    news_url = None
    if tweet['entities']['urls']:
        news_url = tweet['entities']['urls'][0]['expanded_url']

    if 'retweeted_status' in list(tweet.keys()):
        retweet_count = tweet['retweeted_status']['retweet_count']
        favorite_count = tweet['retweeted_status']['favorite_count']
        text = tweet['retweeted_status']['text']
        tweet_url = 'https://twitter.com/' + tweet['retweeted_status']['user']['screen_name'] + '/status/' + \
                    tweet['retweeted_status']['id_str']
        if tweet['retweeted_status']['entities']['urls'] and tweet['retweeted_status']['entities']['urls'][0]['url'].startswith('https'):
            news_url = tweet['retweeted_status']['entities']['urls'][0]['expanded_url']
    return tweet['id'], created_at, day, hour, int(retweet_count), int(favorite_count), text, tweet_url, news_url, tweet


def get_news_url(keyword):
    tso = TwitterSearchOrder()  # create a TwitterSearchOrder object
    tso.set_keywords([keyword])  # let's define all words we would like to have a look for
    tso.set_language('en')  # we want to see English tweets only
    # tso.set_result_type('mixed')
    # tso.set_negative_attitude_filter()
    tso.set_link_filter()
    ts = TwitterSearch(
        consumer_key="your key",  
        consumer_secret="your secret",  
        access_token="your token",  
        access_token_secret="your token secret"  
    )

    list_tweets = []
    tweets = ts.search_tweets_iterable(tso)
    for tweet in tweets:
        list_tweets.append(tweet)

    list_info = list(map(get_tweet_info, list_tweets))
    list_url = []

    for row in list_info:
        print(row)
        if row[5] < 1000 or exclude_url(row[8]):
            continue
        list_url.append((row[0], row[4], row[5], row[7], row[8]))
    return list(set(list_url))


if __name__ == '__main__':
    NEWS_API_DIR = '../static/newsAPI/'
    interval = 24 * 3600
    while True:
        print(todayString() + ' starts with twitter API')
        with open(NEWS_API_DIR + todayString() + '_keywords.txt', 'r') as f:
            keywords = f.readlines()
        keywords = list(map(lambda x: x.replace('\n', ''), keywords))

        TW_DIR = '../static/data/'
        try:
            os.makedirs(TW_DIR)
        except OSError:
            pass

        data = {}
        print(keywords)
        with open(TW_DIR + todayString() + '_predata.csv', 'w') as outfile:
            writer = csv.writer(outfile, delimiter=',')
            for keyword in keywords:
                urls = get_news_url(keyword)
                if len(urls) == 0:
                    continue
                data[keyword] = urls
                print(urls)
                for row in data[keyword]:
                    print(row)
                    writer.writerow((row[0], keyword, row[1], row[2], row[3], row[4]))
                print("finish twitter search for {}".format(keyword))
                print()

        df = pd.read_csv(TW_DIR + todayString() + '_predata.csv',
                         names=['tweet_id', 'news_keyword', 'retweet_count', 'favorite_count', 'tweet_url', 'news_url']).drop(['tweet_id'], axis=1)
        df = df.drop_duplicates(['news_url', 'tweet_url'])
        df = df.loc[df.groupby(['news_url'])['favorite_count'].idxmax()]
        df['news_id'] = df.index.to_series().map(lambda x: uuid4())
        df = df[['news_id', 'news_keyword', 'news_url', 'tweet_url', 'retweet_count', 'favorite_count']]
        df.to_csv(TW_DIR + todayString() + '_data.csv', header=False, index=False, line_terminator='\n')
        print('complete')
        time.sleep(interval)
