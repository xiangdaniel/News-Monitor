import os
import re
import requests
import string
import json
from nltk.corpus import stopwords
from datetime import datetime
from newsapi import NewsApiClient
import time


def tokenize(line):
    if line is None:
        return None
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    res = []
    for w in wordsep.split(line):
        res.append(w.lower())
    return res


def flatten(listOfList):
    if listOfList is None:
        return None
    res = []
    for l in listOfList:
        if l is not None:
            res.extend(l)
    return res


def removeStopwords(wordList):
    with open('../resource/stopwords.txt') as f:
        customStopwords = f.readlines()
    customStopwords = list(map(lambda x: x.replace('\n', ''), customStopwords))
    # print(stopwords)
    wordList = list(filter(lambda x: x != None, wordList))
    wordList = list(map(
        lambda x: x if x not in customStopwords and x not in stopwords.words('english') else '', wordList))
    wordList = list(filter(lambda x: x != '', wordList))
    return wordList
    # print(wordList)
# removeStopwords(['1', 'are'])


def todayString():
    return datetime.today().strftime('%Y-%m-%d')


def getNews():
    # Init
    newsapi = NewsApiClient(api_key='ee08a30409564f7ba6005268971d1c6e')

    # /v2/top-headlines
    top_headlines = newsapi.get_top_headlines(language='en') # country='us'
    articles = top_headlines['articles']

    # /v2/everything
    # all_articles = newsapi.get_everything(q='bitcoin',
    #                                       sources='bbc-news,the-verge',
    #                                       domains='bbc.co.uk,techcrunch.com',
    #                                       from_param='2017-12-01',
    #                                       to='2017-12-12',
    #                                       language='en',
    #                                       sort_by='relevancy',
    #                                       page=2)
    #
    # /v2/sources
    # sources = newsapi.get_sources()
    return articles


# def getNews():
#     articles = []
#     for pageId in range(0, 10):
#         url = 'https://newsapi.org/v2/top-headlines?' +\
#         'country=ca&' +\
#         'pageSize=100&' +\
#         'page={}&'.format(pageId) +\
#         'apiKey=f49a1244453e4f1987bab08f8c6d71f9'
#         response = requests.get(url).json()
#         # print(response)
#         articles.extend(response['articles'])
#     return articles

# def getNews():
#     articles = []
#     for pageId in range(0, 10):
#         url = 'https://newsapi.org/v2/everything?' +\
#         'q=&' +\
#         'from=2019-02-23&' +\
#         'to=2019-03-02&' +\
#         'sortBy=popularity&' +\
#         'pageSize=100&' +\
#         'page={}&'.format(pageId+1) +\
#         'apiKey=f49a1244453e4f1987bab08f8c6d71f9'
#         response = requests.get(url).json()
#         print(response)
#         articles.extend(response['articles'])
#     return articles

# def getNews():
#     articles = []
#     for pageId in range(0, 10):
#         url = 'https://newsapi.org/v2/top-headlines?' +\
#         'country=ca&' +\
#         'language=en&' +\
#         'pageSize=100&' +\
#         'page={}&'.format(pageId) +\
#         'apiKey=ee08a30409564f7ba6005268971d1c6e'
#         response = requests.get(url).json()
#         print(response)
#         #articles.extend(response['articles'])
#     return articles


if __name__ == '__main__':
    articles = getNews()
    NEWS_API_DIR = '../static/newsAPI/'
    interval = 24 * 3600
    try:
        os.makedirs(NEWS_API_DIR)
    except OSError:
        pass

    while True:
        print(todayString() + ' starts with News API')
        with open(NEWS_API_DIR + todayString() + '_articles.json', 'w') as f:
            f.write(json.dumps(articles))

        lines = list(map(lambda x: x['title'][:x['title'].rfind('-')], articles))
        print(len(lines))
        # lines.extend(list(map(lambda x: x['description'], articles)))
        # lines.extend(list(map(lambda x: x['content'], articles)))

        with open(NEWS_API_DIR + todayString() + '_titles.txt', 'w') as f:
            f.write('\n'.join(lines))
        print('complete')
        time.sleep(interval)
