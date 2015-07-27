""" This module extracts the topics from tweets.csv """

from __future__ import division
import json
import collections
import csv
import os
import logging
import cPickle as pickle
import re, base64
import helpers

from nltk.corpus import stopwords
from gensim import corpora,models

from deepthought import config

def cleaner(text):
   	def strip_emojis(tl):
		myre = re.compile(
            u'['u'\U0001f300-\U0001ffff'u'\U0001f600-\U0001f64f'u'\U0001f680-\U0001f6ff'u'\u2600-\u26ff\u2700-\u27bf]+',
            re.UNICODE)
		return myre.sub('', ' '.join(tl)).split(' ')
	def strip_escape(text):
		while True:
			if text[:1] == '\n':
				text = text[1:]
			else:
				break
		return text
	def strip_others(text):
		t_stop = ['rt','#','http','@']
		for a in t_stop:
			if a in text:
				return False
		return True

	tl = unicode(text.lower()).split(' ')
	tl = strip_emojis(tl)
   	tl = filter(lambda w: (not w in stop), tl)
   	tl = map(strip_escape, tl)
   	tl = filter(strip_others, tl)
   	return ' '.join(tl)


class LanguageProcesser():
    def __init__(self, f_p):
        self.fp = f_p
        self.tweets_fp = os.path.join(f_p,'tweets.csv')
    def process(self):
        f = open(self.tweets_fp, 'rb')
        reader = csv.DictReader(f)
        g = open('.temp','w')
        for tweet in tweets_reader:
            g.write(cleaner((json.loads(tweet['tweet']))['text'] + '\n'))
        g.close()
        f = open('.temp','rb')
        dict = corpora.Dictionary(line[:-1].lower().split() for line in f)
        once_ids = [tokenid for tokenid, docfreq in dict.iteritems() if docfreq == 1]
        dict.filter_tokens(once_ids)
        dict.compactify()
        pickle.dump(dict, open('.tempdict', 'w'))
        f.close()
        f = open(self.tweets_fp, 'rb')
        corpus = [dict.doc2bow(line.split(' ')) for line in f]
        corpora.MmCorpus.serialize(('.tempcorp'), corpus)
        tfidf = models.TfidfModel(corpus = corpus, dictionary = dict)
        corpus_Tfidf = tfidf[corpus]
        lsi = lsimodel.LsiModel(corpus_Tfidf, id2word=dict, num_topics = 200)
        g = open(os.path.join(self.fp,'.topics','w'))
        g.write(os.path.join(self.fp, lsi[doc_tfidf]))
