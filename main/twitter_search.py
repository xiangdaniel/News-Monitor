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
    # print(tweet)
    if tweet['entities']['urls']:
        news_url = tweet['entities']['urls'][0]['expanded_url']

    if 'retweeted_status' in list(tweet.keys()):
        retweet_count = tweet['retweeted_status']['retweet_count']
        favorite_count = tweet['retweeted_status']['favorite_count']
        text = tweet['retweeted_status']['text']
        tweet_url = 'https://twitter.com/' + tweet['retweeted_status']['user']['screen_name'] + '/status/' + \
                    tweet['retweeted_status']['id_str']
        # if 'media' in tweet['retweeted_status']['entities'].keys():
        #     news_url = tweet['retweeted_status']['entities']['media'][0]['expanded_url']
        if tweet['retweeted_status']['entities']['urls'] and tweet['retweeted_status']['entities']['urls'][0]['url'].startswith('https'):
            news_url = tweet['retweeted_status']['entities']['urls'][0]['expanded_url']

        # if tweet['id'] == 1111483393781317638:
        #     print(data)
    return tweet['id'], created_at, day, hour, int(retweet_count), int(favorite_count), text, tweet_url, news_url, tweet


def get_news_url(keyword):
    tso = TwitterSearchOrder()  # create a TwitterSearchOrder object
    tso.set_keywords([keyword])  # let's define all words we would like to have a look for
    tso.set_language('en')  # we want to see English tweets only
    # tso.set_result_type('mixed')
    # tso.set_negative_attitude_filter()
    tso.set_link_filter()
    ts = TwitterSearch(
        consumer_key="zdxmxeAtMbWFaE1kGBT7vLNVt",  # aY5Lo6Xz9V8WEiMD9gCXkacYx zdxmxeAtMbWFaE1kGBT7vLNVt
        consumer_secret="EPb5S6y9vCGr1qHbhSdcwiaCBbAszDdJ12cdrLXzlzdFEdFuek",  # dtc6GeUqTSsel8YqqjJFAcA8Jupr7pYBHZVBSiRYBMuLzE5k1g EPb5S6y9vCGr1qHbhSdcwiaCBbAszDdJ12cdrLXzlzdFEdFuek
        access_token="1112141420108759042-KAOY2m0mAju39MbHJrnH7MBuU55C9Y",  # 2865195647-BZAx398on0JTNPA5ejjpFxJOLek7DsfBD06vWqg 1112141420108759042-KAOY2m0mAju39MbHJrnH7MBuU55C9Y
        access_token_secret="yWeARXoomgrtv316s9vHBILvFG7VbKLK3Njyyr3rhkFle"  # NjFAvDrzY6jC7PZ4NCneuZGouccFbTUty95mMlGfkO75t yWeARXoomgrtv316s9vHBILvFG7VbKLK3Njyyr3rhkFle
    )
    # ts = TwitterSearch(
    #     consumer_key="aY5Lo6Xz9V8WEiMD9gCXkacYx",
    #     consumer_secret="dtc6GeUqTSsel8YqqjJFAcA8Jupr7pYBHZVBSiRYBMuLzE5k1g",
    #     access_token="2865195647-BZAx398on0JTNPA5ejjpFxJOLek7DsfBD06vWqg",
    #     access_token_secret="NjFAvDrzY6jC7PZ4NCneuZGouccFbTUty95mMlGfkO75t"
    # )

    list_tweets = []
    tweets = ts.search_tweets_iterable(tso)
    for tweet in tweets:
        list_tweets.append(tweet)
        # print(tweet)

    # list(map(lambda x: (x['created_at'], x['retweet_count']), data_list))
    # Fri Mar 01 23:59:55 +0000 2019

    list_info = list(map(get_tweet_info, list_tweets))
    list_url = []
    # print(data_list)
    # with open(TW_DIR + todayString() + '-' + keyword + '-urls.csv', 'w') as out:
    #     csv_out = csv.writer(out)
    #     # csv_out.writerow(['Created_at', 'Date', 'Time', 'retweet_count', 'favorite_count', 'text', 'url', 'id', 'te'])
    #     for row in data_list:
    #         if row[3] < 1000 or exclude_url(row[6]):
    #             continue
    #         csv_out.writerow(row)

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
        # keywords = ['Lori Lightfoot']
        with open(TW_DIR + todayString() + '_predata.csv', 'w') as outfile:
            writer = csv.writer(outfile, delimiter=',')
            for keyword in keywords:
                # print(keyword)
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
        # df = df.groupby(['news_url', 'tweet_url']).agg({'news_keyword': ' / '.join})
        # df = df.groupby(['news_keyword', 'news_url', 'tweet_url']).agg({'favorite_count': 'max'})
        # df = df.reset_index()
        # df['news_id'] = df.index.to_series().map(lambda x: uuid4())
        # df = df.set_index('news_id')
        df.to_csv(TW_DIR + todayString() + '_data.csv', header=False, index=False, line_terminator='\n')
        print('complete')
        time.sleep(interval)
