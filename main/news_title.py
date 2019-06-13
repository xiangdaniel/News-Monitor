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


def todayString():
    return datetime.today().strftime('%Y-%m-%d')


def getNews():
    # Init
    newsapi = NewsApiClient(api_key='input your key')

    top_headlines = newsapi.get_top_headlines(language='en') # country='us'
    articles = top_headlines['articles']
    return articles


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
