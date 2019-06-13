from kafka import KafkaProducer
from datetime import datetime
import time, threading, csv


def todayString():
    return datetime.today().strftime('%Y-%m-%d')


def send_at(rate):
    # rand = random.Random()  # docs say threadsafe, but construct one per thread anyway
    # producer = KafkaProducer(bootstrap_servers=['199.60.17.210:9092', '199.60.17.193:9092'])
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic1 = 'nlp-1'
    topic2 = 'mlnews-1'
    interval = 60
    sent_ml = False
    total_time = 0
    TW_DIR = '../static/data/'
    while True:
        print('start')
        msg = ''
        msg_ml = ''
        if total_time == 24 * 3600:
            sent_ml = False  # send news at a new day to ml model
            total_time = 0  # reset total_time
        with open(TW_DIR + todayString() + '_data.csv') as f:
            rows = csv.reader(f, delimiter='\n')
            for row in rows:
                msg += row[0]
                msg += ';'
                list_ml = row[0].split(',')
                msg_ml += list_ml[0] + ',' + list_ml[2]
                msg_ml += ';'
                # break
            msg = msg[:-1]
            msg_ml = msg_ml[:-1]
            # print(msg_ml)
        # x, y = data_point_gauss(rand)
        # msg = '%s %s' % (x, y)
        producer.send(topic1, msg.encode("utf-8"))  # .encode('ascii')
        if not sent_ml:
            producer.send(topic2, msg_ml.encode("utf-8"))
            sent_ml = True
        total_time += interval
        time.sleep(interval)


if __name__ == "__main__":
    #for rate in rates:
    rate = 1
    server_thread = threading.Thread(target=send_at, args=(rate,))
    server_thread.setDaemon(True)
    server_thread.start()

    while 1:
        time.sleep(1)
