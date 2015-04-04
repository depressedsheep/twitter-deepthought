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
	d.clean_text(force = True)
	d.create_dict(force = True)
	d.create_corpus(force = True)
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
		'dict': os.path.join('thinking', 'braindict'),
		'corp': os.path.join('thinking', 'braincorp')
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
			print "Raw compressed file does not existing. Downloading."
			print self.key
			self.k.get_contents_to_filename(os.path.join('thinking',savepath + '.gz'))
		self.f = gzip.open(os.path.join('thinking',savepath+'.gz'), 'rb')
		print self.f
		a = json.loads(self.f.readline())
		#print a['text']
	def clean_text(self, force = False): #generate cleaned text
		print "Attempting to clean text..."
		self.ensure_dir(self.dirs['dump'])
		if os.path.exists(os.path.join(self.dirs['dump'], self.key)):
			if not force:
				print "Text already cleaned. Set force to True to force clean."
				pass
			else: 
				print "Forced to clean text."
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
		print "Attempting to create dictionary..."
		self.ensure_dir(self.dirs['dict'])
		self.f_text = open(os.path.join(self.dirs['dump'],self.key),'r')
		if os.path.exists(os.path.join(self.dirs['dict'],self.key)):
			if force == False:
				print "Dictionary already exists. Set force to True to refresh it."
			else:
				print "Forced to create dictionary."
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

		print "Dictionary created."

	def create_corpus(self, force = False):
		print "Attempting to create corpus..."
		self.ensure_dir(self.dirs['corp'])

		if os.path.exists(os.path.join(self.dirs['corp'], self.key)):
			if force == False:
				print ".braincorpus exists already. Set force to True to create it again."
			else:
				"Forced to create corpus."
				self.corpus_creator()
		else:
			self.corpus_creator()

	def corpus_creator(self):
		self.f_dict = open(os.path.join(self.dirs['dict'], self.key),'rb+') #dictionary
		self.f_text = open(os.path.join(self.dirs['dump'], self.key),'rb+')
		self.f_corp = open(os.path.join(self.dirs['corp'], self.key), 'w') #vector corpus
		self.dict = pickle.load(self.f_dict)
		c = 0
		while True:
			c += 1
			#print c
			text = self.f_text.readline()
			if self.f_text.readline() == '':
				print "EOF, no more lines to read."
				break
			vector = self.dict.doc2bow(text.split(' ')) #vector is an ordinary python list
			pickle.dump(vector, self.f_corp)			
		print "Corpus created, and written into thinking/braincorpus/[KEY_NAME]"	


if __name__ == '__main__':
	#print removestopwords('This is a placeholder sentence, because I\'m bored.')
	main()
