import pymongo
from kafka import KafkaConsumer
import time, threading, json
from datetime import datetime


def todayString():
    return datetime.today().strftime('%Y-%m-%d')


def send_at(rate):
    print('Running Consumer..')
    topic = 'nlp-2'
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    mydb = myclient["news"]
    mycoll = mydb["news_data"]
    while True:
        for msg in consumer:
            value = msg.value
            # print(value)
            news_id = value['news_id']
            news_keyword = value['news_keyword']
            retweet_count = value['retweet_count']
            favorite_count = value['favorite_count']
            news_url = value['news_url']
            tweet_url = value['tweet_url']
            tweets_comment = value['tweets_comment']
            tweets_tokens = value['tweets_tokens']
            sentiment_scores = value['sentiment_scores']
            like_counts = value['like_counts']
            comment_time = value['comment_time']
            weighted_sentiment_score = value['weighted_sentiment_score']
            date = todayString()
            ml_ans = {
                'news_id': news_id,
                'news_keyword': news_keyword,
                'retweet_count': retweet_count,
                'favorite_count': favorite_count,
                'news_url': news_url,
                'tweet_url': tweet_url,
                'tweets_comment': tweets_comment,
                'tweets_tokens': tweets_tokens,
                'comments_sentiment_scores': sentiment_scores,
                'like_counts': like_counts,
                'comment_time': comment_time,
                'weighted_sentiment_score': weighted_sentiment_score,
                'date': date
            }
            if (mycoll.find({'news_id': news_id}).count() != 0):
                print('found')
                result = mycoll.update_many(
                    {"news_id": news_id},
                    {
                        "$set": ml_ans,
                        "$currentDate": {"lastModified": True}

                    }
                )
                print("Data updated with news_id", result)
            else:
                print('not found')
                result = mycoll.insert_one(ml_ans)
                print(result.inserted_id)
        #     break
        # break
            # result = parse(html)
            # parsed_records.append(result)
    # consumer.close()
        time.sleep(5)


if __name__ == "__main__":
    rate = 1
    server_thread = threading.Thread(target=send_at, args=(rate,))
    server_thread.setDaemon(True)
    server_thread.start()

    while 1:
        time.sleep(1)
