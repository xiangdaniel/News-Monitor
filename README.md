# Distributed News Monitor System

Distributed News Monitor System is the course project of CMPT 733. 

## Folder introduction
**Folder directory: main**

| File | Description |
| --- | --- |
| `attention.py` | the RNN with attention model implemented with TensorFlow |
| `call_selenium_client.py` | call selenium client for distributed web crawler |
| `kafka_producer.py` | send data to kafka topics for further processing |
| `keywords.py` | use NER to extract news keywords from news headlines |
| `load_tools.py` | Load tools |
| `ml_test.py` | receive the data from Kafka topic mlnews-2 and write to MongoDB |
| `news_nlp.py` | receive the data from Kafka topic mlnews-1, distriuted web crawler and nlp processing for news, write to Kafka topic mlnews-2 |
| `news_title.py` | crawl news headlines from news API |
| `nlp_test.py` | receive the data from Kafka topic nlp-2 and write to MongoDB |
| `selenium_client.py` | selenium client for crawling tweets comments, like_count, etc. |
| `twitter_nlp.py` | receive the data from Kafka topic nlp-1, distriuted web crawler and nlp processing for social engaement, write to Kafka topic nlp-2 |
| `twitter_search.py` | search highly focused tweet with keywords |


## command description
before run ml_testpy.py, please download file from https://drive.google.com/file/d/0B7XkCwpI5KDYNlNUTTlSS21pQmM/edit
and download save_models folder from https://drive.google.com/open?id=1GDZGFfIQm0pN7Td-PbVscj_YFMThUWx2

python news_title.py

python keywords.py

python twitter_search.py

python kafka_producer.py

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.0,org.mongodb:mongo-java-driver:3.10.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 twitter_nlp.py nlp-1

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.0,org.mongodb:mongo-java-driver:3.10.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 news_nlp.py mlnews-1

python nlp_test.py

python ml_test.py
