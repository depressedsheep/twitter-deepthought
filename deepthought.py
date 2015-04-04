#
# Main analysis module for compressed files. Imports crawler.py and aims to define functions 
# to parse and analyse the tweets in various ways, most probably with gensim
# 
# Goals
# - TF-IDF vectors
# - Create a LSA model and update it at every time interval
# - Figure out how to run this concurrently with crawler.py
#
from boto.s3.connection import Location, S3Connection
from boto.s3.key import Key
from gensim import corpora, models, similarities
import logging
import os
from nltk.corpus import stopwords
import gzip
from config import boto_access, boto_secret
import numpy as np
import json
import re
import cPickle as pickle #i don't care even if cPickle is much slower than alternatives like thrift or messagepack; i'm trying to get something done here
import base64
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
stop = stopwords.words('english')

def main():
	d = deepthought('31-03-2015_00')
	d.load('31-03-2015_00')
	d.clean_text(force = False)
	d.create_dict(force = False)
	d.create_corpus(force = False)
	d.create_tfidf(force = True)
	d.create_lsi(force = True)
#
# this part assumes loading from boto
class deepthought(object):
	def __init__(self, key):
		self.conn = S3Connection(boto_access, boto_secret)
		self.bucket = self.conn.get_bucket('twitter-deepthought')
		self.key = key
		self.k = Key(self.bucket)
		self.k.key = self.key
		self.t_stop = ['rt', '#', 'http', '@']	
		self.dirs = {
		'load':'thinking',
		'dump': os.path.join('thinking', 'braindump'),
		'dict': os.path.join('thinking', 'braindict'), #the last three are kind of redundant, will clear soon. were meant for pickling.
		'corp': os.path.join('thinking', 'braincorp'),
		'tfidf': os.path.join('thinking', 'braintfidf'),
		'lsi': os.path.join('thinking','brainlsi')
		}
		self.fp = {
		'lsi': os.path.join(self.dirs['lsi'], self.key + '.lsi'),
		'tfidf': os.path.join(self.dirs['tfidf'], self.key + '.tfidf_model'),
		'corp': os.path.join(self.dirs['corp'], self.key + '.mm')
		}
	def ensure_dir(self,f):
		if not os.path.exists(f):
			os.makedirs(f)
	def print_list(self):
		print "Key list: "
		for z in self.bucket.list():
			print z.name
	def load(self,savepath): 
		#load from boto here 
		self.ensure_dir(self.dirs['load'])
		if not os.path.exists(os.path.join('thinking', savepath + '.gz')):
			logger.info("Raw compressed file does not existing. Downloading.")
			print self.key
			self.k.get_contents_to_filename(os.path.join('thinking',savepath + '.gz'))
		self.f = gzip.open(os.path.join('thinking',savepath+'.gz'), 'rb')
		print self.f
		a = json.loads(self.f.readline())
		#print a['text']
	def clean_text(self, force = False): #generate cleaned text
		logging.info("Attempting to clean text...")
		self.ensure_dir(self.dirs['dump'])
		if os.path.exists(os.path.join(self.dirs['dump'], self.key)):
			if not force:
				logging.info("Text already cleaned. Set force to True to force clean.")
				pass
			else: 
				logging.info("Forced to clean text.")
				self.cleaner()
		else:
			self.cleaner()

	def cleaner(self):
		self.f_text = open(os.path.join(self.dirs['dump'], self.key), 'wb')
		for tweet in self.f:
				tweet = json.loads(tweet)
				text = tweet['text']
				text = self.clean(text) #returned as a list
				self.f_text.write(' '.join(text).encode('ascii','ignore') + '\n')
		self.f_text.close()
		logging.info("Text cleaned.")
	def clean(self, rawtext):
		tl = unicode(rawtext.lower()).split(' ')
		tl = self.strip_emojis(tl)
		tl = filter(lambda w: (not w in self.t_stop), tl)
		tl = filter(lambda w: (not w in stop), tl)
		tl = map(self.strip_escape ,tl)
		tl = filter(self.strip_others, tl)		
		return tl

	def strip_emojis(self,tl):
		myre = re.compile(u'['u'\U0001f300-\U0001ffff'u'\U0001f600-\U0001f64f'u'\U0001f680-\U0001f6ff'u'\u2600-\u26ff\u2700-\u27bf]+', re.UNICODE)
		return myre.sub('', ' '.join(tl)).split(' ')

	def strip_escape(self, text):
		while True:
			if text[:1] == '\n':
				text = text[1:]
			else:
				break
		return text

	def strip_others(self, text):
		#
		# For now, remove all hashtags and links because the focus now is going to be only on the words.
		#
		for a in self.t_stop:
			if a in text:
				#print text
				return False
		return True

	def create_dict(self, force = False):
		logging.info("Attempting to create dictionary...")
		self.ensure_dir(self.dirs['dict'])
		self.f_text = open(os.path.join(self.dirs['dump'],self.key),'r')
		if os.path.exists(os.path.join(self.dirs['dict'],self.key)):
			if force == False:
				logging.info("Dictionary already exists. Set force to True to refresh it.")
			else:
				logging.info("Forced to create dictionary.")
				self.dict_creator()
		else:
			self.dict_creator()

	def dict_creator(self):
		self.f_dict = open(os.path.join(self.dirs['dict'], self.key), 'w')

		self.dict = corpora.Dictionary(line[:-1].lower().split() for line in self.f_text)
		once_ids = [tokenid for tokenid,docfreq in self.dict.iteritems() if docfreq == 1]
		self.dict.filter_tokens(once_ids)
		self.dict.compactify()

		pickle.dump(self.dict, self.f_dict) #this is a dump of the dictionary
		print self.dict

		self.f_text.close()
		self.f_dict.close()

		logging.info("Dictionary created.")

	def create_corpus(self, force = False):
		logging.info("Attempting to create corpus...")
		self.ensure_dir(self.dirs['corp'])

		if os.path.exists(self.fp['corp']):
			if force == False:
				logging.info("Corpus exists already. Set force to True to create it again.")
			else:
				logging.info("Forced to create corpus.")
				self.corpus_creator()
		else:
			self.corpus_creator()

	def corpus_creator(self):
		self.f_dict = open(os.path.join(self.dirs['dict'], self.key),'rb+') #dictionary
		self.f_text = open(os.path.join(self.dirs['dump'], self.key),'rb+')
		self.dict = pickle.load(self.f_dict)
		corpora.MmCorpus.serialize(self.fp['corp'], [self.dict.doc2bow(line.split(' ')) for line in self.f_text])
	
		logging.info("Corpus created, and written into thinking/braincorpus/[KEY_NAME].mm in the Market Matrix format.")
		self.f_text.close()
		self.f_dict.close()
	def create_tfidf(self, force = False):
		logging.info("Attempting to create tf-idf model.")
		self.ensure_dir(self.dirs['tfidf'])
		if not os.path.exists(os.path.join(self.dirs['tfidf'], self.key + '.tfidf_model')):
			self.tfidf_creator()
		else:
			if force == True:
				logging.info("Forced to create Tf-idf model.")
				self.tfidf_creator()
			else:
				logging.info("Tf-idf model already created. Set force to True to create again.")
	def tfidf_creator(self):
		logging.info("Tf-idf model initialised.")
		logging.info("Attempting to convert current corpus to the Tf-idf model...")
		self.f_corp = open(os.path.join(self.dirs['corp'], self.key + '.mm'), 'r')
		self.corpus = corpora.MmCorpus(self.f_corp)
		#print self.corpus

		self.tfidf = models.TfidfModel(corpus = self.corpus, dictionary = pickle.load(open(os.path.join(self.dirs['dict'], self.key))))
		self.corpus_tfidf = self.tfidf[self.corpus]
		print self.tfidf
		self.tfidf.save(self.fp['tfidf'])
		logging.info("Tf-idf model created, saved in tfidf_model format.")

	def create_lsi(self, force = False):
		#
		# Current approach: generate a seperate LSI / LSA for each time block, then compare over time
		# After initial development and for research purposes, consider collecting a few days worth of data, before creating a 'master' dictionary and 'corpus' to create a LSI/LSA model for topic-modelling 
		# In that case, ensure memory efficiency (i.e. not loading large chunks of data into memory), and possibly use a different organisational structure
		# Currently not very sure, but I'm guessing you'd add an option into the vector generator function to use the 'master' dict, and then add into the LSA structure
		#
		self.ensure_dir(self.dirs['lsi'])
		if os.path.exists(os.path.join(self.dirs['lsi'], self.key + '.lsi')):
			if force == True:
				logging.info("Forced to create LSI/LSA model again.")
				self.lsi_creator()
			else:
				logging.info("LSI/LSA model already created. Set force to True to create LSI model again.")
				pass
		else:
			self.lsi_creator()
	def lsi_creator(self, document_size = 1000):
		self.tfidf.load(self.fp['tfidf'])
		print self.tfidf
		self.dict = pickle.load(open(os.path.join(self.dirs['dict'], self.key)))
		self.lsi = models.LsiModel(self.corpus_tfidf, id2word = self.dict, num_topics = 200)
		self.corpus_lsi = self.lsi[self.corpus_tfidf] #double wrapper over the original corpus
		self.lsi.print_topics(5)
		self.lsi.save(self.fp['lsi'])
		logging.info("LSA model created.")
	def display_lsi(self):
		self.lsi.load(self.fp['lsi'])
		lsi.print_topics(10)
if __name__ == '__main__':
	main()
