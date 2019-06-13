import spacy
from spacy import displacy
import en_core_web_sm
from collections import Counter
from datetime import datetime
import time

def todayString():
    return datetime.today().strftime('%Y-%m-%d')


# def preprocess(sent):
#     sent = nltk.word_tokenize(sent)
#     sent = nltk.pos_tag(sent)
#     return sent


def get_keywords(titles):
    # ne_tree = ne_chunk(pos_tag(word_tokenize(sentence)))
    # sent = preprocess(sentence)
    # pattern = 'NP: {<DT>?<JJ>*<NN>}'
    # cp = nltk.RegexpParser(pattern)
    # cs = cp.parse(sent)
    # NPChunker = nltk.RegexpParser(pattern) 
    # result = NPChunker.parse(sent)
    # iob_tagged = tree2conlltags(cs)
    nlp = en_core_web_sm.load()
    doc = nlp(titles)
    label_list = ['PERSON', 'NORP', 'FAC', 'ORG', 'GPE', 'PRODUCT', 'EVENT']
    keywords = []
    for ent in doc.ents:
        if ent.label_ in label_list:
            keywords.append(ent.text)
    return list(set(keywords))


if __name__ == '__main__':
    NEWS_API_DIR = '../static/newsAPI/'
    interval = 24 * 3600
    while True:
        print(todayString() + ' starts with keyword extraction')
        with open(NEWS_API_DIR + todayString() + '_titles.txt', 'r') as f:
            titles = f.readlines()
        titles = list(map(lambda x: x.replace('\n', ''), titles))

        keywords = get_keywords(' '.join(titles))
        with open(NEWS_API_DIR + todayString() + '_keywords.txt', 'w') as f:
            f.write('\n'.join(keywords))
        print('complete')
        time.sleep(interval)
