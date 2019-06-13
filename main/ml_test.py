from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pymongo
from kafka import KafkaConsumer
import time, threading, json
from datetime import datetime
import tensorflow as tf
import keras
import numpy as np
from tensorflow.examples.tutorials.mnist import input_data
import os
import io
from os import listdir
from os.path import isfile, join
import random
import shutil
import pandas as pd
from collections import defaultdict
import re
import gensim

import sys
from scipy import sparse

from keras.preprocessing.text import Tokenizer, text_to_word_sequence
from keras.preprocessing.sequence import pad_sequences
from keras.utils.np_utils import to_categorical
import time

from attention import *
from tensorflow.python.ops.rnn import bidirectional_dynamic_rnn as bi_rnn
from tensorflow.contrib.rnn import GRUCell

MAX_SENT_LENGTH = 100
MAX_SENTS = 100 # this is the longest number of sentences in a document!!!
MAX_NB_WORDS = 60000
EMBEDDING_DIM = 300 #due to word2vec dimension!!!
HIDDEN_SIZE = 50 #based on Yang et al CMU. (Hierachical Attention Networks for Document Classification)
ATTENTION_SIZE = 100 #same as Yang et al.
BATCH_SIZE = 50
NUM_ITERS = 100
DISPLAY_STEP = 10
VALIDATION_STEP = 10
TESTING_BATCH = 10
USER_EMBEDDING_DIM = 200


def todayString():
    return datetime.today().strftime('%Y-%m-%d')


def fit_tokenizer_based_on_training(train_folder, test_folder, dict_selected_words,
                                    ground_truth_file='snopes_ground_truth.csv'):
    df = pd.read_csv(ground_truth_file)
    full = zip(df['snopes_page'], df['claim_label'])
    dict_url_ground_truth = {}
    for url, label in full:
        assert label == True or label == False
        if label == True:
            dict_url_ground_truth[url] = 1
        elif label == False:
            dict_url_ground_truth[url] = 0
    assert len(dict_url_ground_truth) == 562

    def read_text_files(infolder):
        documents = [fn for fn in listdir(infolder) if fn.endswith('.txt')]
        data = np.zeros((len(documents), MAX_SENTS, MAX_SENT_LENGTH), dtype='int32')
        dict_docs = {}
        for fn in documents:
            p = join(infolder, fn)
            fin1 = open(p, 'r')
            url = fin1.readline().replace('\n', '')
            assert 'http' in url
            sents = []
            for line in fin1:
                sents.append(line.replace('\n', ''))
            dict_docs[url] = sents

        Y = []
        for idx, url in enumerate(dict_docs.keys()):
            assert 'http' in url
            label = dict_url_ground_truth[url]
            assert label == 0 or label == 1
            Y.append([label, 1 - label])
            sentences = dict_docs[url]
            for j, sent in enumerate(sentences):
                if j < MAX_SENTS:
                    wordTokens = text_to_word_sequence(sent)
                    k = 0
                    for _, word in enumerate(wordTokens):
                        if word not in dict_selected_words:
                            continue
                        index_of_word = dict_selected_words[word]
                        assert index_of_word >= 1 and index_of_word <= 16000
                        if k < MAX_SENT_LENGTH:
                            data[idx, j, k] = index_of_word
                            k += 1

        return data, np.array(Y)

    X_train, y_train = read_text_files(infolder=train_folder)
    X_test, y_test = read_text_files(infolder=test_folder)
    print(y_train.shape, "here")
    return X_train, y_train, X_test, y_test


def load_url_content_text2num(selected_words_file='out_top_16K_words_file.txt'):
    parent = "train_test_data"
    dict_folds = {}
    fin = open(selected_words_file, 'r')
    cnt = 1
    dict_selected_words = {}
    for line in fin:
        _, w, _ = line.split()
        dict_selected_words[w] = cnt
        cnt += 1
    assert len(dict_selected_words) == 16000 and max(dict_selected_words.values()) == 16000
    fin.close()

    # for i in range(0, 5):
    #     #########################################################
    #     dict_words = {}  # for stat
    #     data_i = '%s/data_%s' % (parent, i)
    #     train_folder = '%s/train' % data_i
    #     test_folder = '%s/test' % data_i
    #     X_train, y_train, X_test, y_test = fit_tokenizer_based_on_training(train_folder, test_folder,
    #                                                                        dict_selected_words)
    #     dict_folds[i] = (X_train, y_train, X_test, y_test)

    return dict_folds, dict_selected_words


def buildModel():
    word2vec = gensim.models.KeyedVectors.load_word2vec_format('GoogleNews-vectors-negative300.bin.gz', binary=True)
    dict_folds, dict_selected_words = load_url_content_text2num()
    no_vocabs = 16000
    embedding_matrix = np.random.random((no_vocabs, EMBEDDING_DIM))

    for word, i in dict_selected_words.items():
        if word in word2vec.wv.vocab:
            embedding_vector = word2vec[word]
            embedding_matrix[i - 1] = embedding_vector

    tf.reset_default_graph()
    embedding_matrix_variable = tf.Variable(embedding_matrix, trainable=True, dtype=tf.float32)
    batch_sent_ph = tf.placeholder(tf.int32, [None, MAX_SENT_LENGTH], name="batch_sent_ph")
    batch_sent_embedded = tf.nn.embedding_lookup(embedding_matrix_variable, batch_sent_ph)
    y_ph = tf.placeholder(tf.float32, [None, 2], name="labels")
    sentence_length_ph = tf.placeholder(tf.int32, [None], name="sentence_length_ph")
    doc_actual_length_ph = tf.placeholder(tf.int32, [None], name="doc_actual_length_ph")
    with tf.variable_scope("first_bi_rnn"):
        rnn_outputs, _ = bi_rnn(GRUCell(HIDDEN_SIZE), GRUCell(HIDDEN_SIZE),
                                inputs=batch_sent_embedded,
                                sequence_length=sentence_length_ph,
                                dtype=tf.float32)
    with tf.name_scope("attention_first_bi_rnn"):
        attention_output, alphas_first = attention(rnn_outputs, ATTENTION_SIZE, return_alphas=True)
    with tf.name_scope("dropout_afterfirst_bi_rnn"):
        dropout_first_bi_rnn = tf.nn.dropout(attention_output, keep_prob=0.8)
    with tf.name_scope("sent_bedding_after_first_birnn"):
        sent_bedding_after_first_birnn = tf.reshape(dropout_first_bi_rnn, shape=[-1, MAX_SENTS, 2*HIDDEN_SIZE])
    ###########second bi-rnn-layer ############################
    with tf.variable_scope("second_bi_rnn"):
        bi_rnn_sent_outputs, _ = bi_rnn(GRUCell(2*HIDDEN_SIZE), GRUCell(2*HIDDEN_SIZE),
                                        inputs=sent_bedding_after_first_birnn,
                                        sequence_length=doc_actual_length_ph,
                                        dtype=tf.float32)
    with tf.name_scope("attention_second_bi_rnn"):
        attention_output2, alphas_second = attention(bi_rnn_sent_outputs, ATTENTION_SIZE, return_alphas=True)
    with tf.name_scope("dropout_after_second_bi_rnn"):
        dropout_second_bi_rnn = tf.nn.dropout(attention_output2, keep_prob=0.8)

    with tf.name_scope("FC_layer"):
        W = tf.Variable(tf.random_normal([HIDDEN_SIZE * 4, 2], stddev=0.1))
        b = tf.Variable(tf.constant(0.1, shape=[1]))
        y_hat = tf.matmul(dropout_second_bi_rnn, W) + b
    with tf.name_scope("softmax"):
        out_softmax = tf.nn.softmax(logits=y_hat)

    with tf.name_scope("loss_cross_entropy"):
        loss = -tf.reduce_mean(tf.reduce_sum(tf.cast(y_ph, tf.float32) * tf.log(out_softmax), axis=1))
    A = tf.summary.scalar("Loss", loss)

    with tf.variable_scope("Traininig"):
        train_step = tf.train.AdamOptimizer(1e-4).minimize(loss=loss)

    with tf.variable_scope("evaluation"):
        ground_truth = tf.argmax(y_ph, 1)
        predicted = tf.argmax(out_softmax, 1)
        correct_prediction = tf.equal(predicted, ground_truth)
        accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
        accuracy_test = tf.reduce_sum(tf.cast(correct_prediction, tf.float32))

    valVar = tf.Variable(0.0, "valVar")
    valVal_ph = tf.placeholder(tf.float32, [], name="independent")
    update_valVar = valVar.assign(valVal_ph)
    mySummary = tf.summary.scalar("Validation", update_valVar)

    B = tf.summary.scalar("Accuracy", accuracy)
    summary_op = tf.summary.merge([A, B])
    placeholder_input = (batch_sent_ph, y_ph, sentence_length_ph, doc_actual_length_ph, valVal_ph)
    components = [accuracy, accuracy_test, update_valVar, mySummary,
                  summary_op, train_step, ground_truth, predicted, alphas_first, alphas_second]
    return components, placeholder_input


def fit_tokenizer_based_on_test(dict_selected_words, news_input):
    def read_text_files(news_input):
        data = np.zeros((1, MAX_SENTS, MAX_SENT_LENGTH), dtype='int32')
        sentences = news_input
        for j, sent in enumerate(sentences):
            if j < MAX_SENTS:
                wordTokens = text_to_word_sequence(sent)
                k = 0
                for _, word in enumerate(wordTokens):
                    if word not in dict_selected_words:
                        continue
                    index_of_word = dict_selected_words[word]
                    assert index_of_word >= 1 and index_of_word <= 16000
                    if k < MAX_SENT_LENGTH:
                        data[j, k] = index_of_word
                        k += 1
        return data

    X_test = read_text_files(news_input)
    return X_test


def predict_news(news_input):
    dict_folds, dict_selected_words = load_url_content_text2num()
    X_test = fit_tokenizer_based_on_test(dict_selected_words, news_input)
    MODEL_PATH = "./saved_models/bestBiGRU_attention_log_result_1"
    components, placeholder_input = buildModel()
    batch_sent_ph, y_ph, sentence_length_ph, doc_actual_length_ph, valVal_ph = placeholder_input
    _, _, _, _, _, _, _, predicted, alphas_first, alphas_second = components
    saver = tf.train.Saver()
    num_docs = 20
    with tf.Session() as sess:
        saver.restore(sess, "./saved_models/bestBiGRU_attention_log_result_1")
        x_batch_test = X_test[:num_docs]
        temp = np.sum(x_batch_test, axis=2)
        doc_actual_lengths_test = np.count_nonzero(temp, axis=1)
        x_batch_reshaped = x_batch_test.reshape(-1, MAX_SENT_LENGTH)
        sentence_actual_lengths_test = np.count_nonzero(x_batch_reshaped, axis=1)
        alphas_first_test, alphas_second_test, predicted = sess.run([alphas_first, alphas_second, predicted],
                                                                    feed_dict={batch_sent_ph: x_batch_reshaped,
                                                                               sentence_length_ph: sentence_actual_lengths_test,
                                                                               doc_actual_length_ph: doc_actual_lengths_test})

    assert predicted[0] == 1 or predicted[0] == 0
    if predicted[0] == 1:
        predicted_label = 'Fake'
    else:
        predicted_label = 'True'

    return predicted_label


def send_at(rate):
    print('Running Consumer..')
    topic = 'mlnews-2'
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
            news_title = value['news_title']
            news_text = value['news_text']
            news_image = value['news_image']
            news_content = value['news_tokens']
            sentiment_scores = value['sentiment_scores']
            label = predict_news(news_content)
            date = todayString()
            ml_ans = {
                'news_id': news_id,
                'news_title': news_title,
                'news_text': news_text,
                'news_image': news_image,
                'news_tokens': news_content,
                'news_sentiment_scores': sentiment_scores,
                'date': date,
                'prediction': label
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
