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
	d.clean_text(force_clean = False)
	d.create_dict(force_dict = False)
	d.create_corpus(force_corpus = True)
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
	
	def print_list(self):
		print "Key list: "
		for z in self.bucket.list():
			print z.name
	def load(self,savepath): 
		#load from boto here and 
		if not os.path.exists('thinking'):
			os.makedirs('thinking')
		if not os.path.exists(os.path.join('thinking', savepath + '.gz')):
			print self.key
			self.k.get_contents_to_filename(os.path.join('thinking',savepath + '.gz'))
		self.f = gzip.open(os.path.join('thinking',savepath+'.gz'), 'rb')
		print self.f
		a = json.loads(self.f.readline())
		#print a['text']
	def create_dict(self, force_dict = False):
		print "Attempting to create dictionary..."
		self.f_text = open('.braindump', 'r')
		if os.path.exists('.braindict'):
			if force_dict == False:
				print "Dictionary already exists. Set force_dict to True to refresh it."
			else:
				print "Forced to create dictionary."
				self.dict_creator()
		else:
			self.dict_creator()
	def dict_creator(self):
		self.dict = corpora.Dictionary(line[:-1].lower().split() for line in self.f_text)
		once_ids = [tokenid for tokenid,docfreq in self.dict.iteritems() if docfreq == 1]
		self.dict.filter_tokens(once_ids)
		self.dict.compactify()
		pickle.dump(self.dict, open('.braindict', 'w')) #this is a dump of the dictionary
		print self.dict
		self.f_text.close()
		print "Dictionary created."
	def create_corpus(self, force_corpus = False):
		print "Attempting to create corpus..."
		if os.path.exists('.braincorpus'):
			if force_corpus == False:
				print ".braincorpus exists already. Set force_corpus as True to create it again."
			else:
				"Forced to create corpus."
				self.corpus_creator()
		else:
			self.corpus_creator()
	def corpus_creator(self):
		self.f_dict = open('.braindict', 'rb+') #dictionary
		self.f_text = open('.braindump','rb+')
		self.f_corp = open('.braincorpus', 'w') #vector corpus
		self.dict = pickle.load(self.f_dict)
		c = 0
		while True:
			c += 1
			#print c
			text = self.f_text.readline()
			if self.f_text.readline() == '':
				print "EOF, no more lines to read."
				break
			vector = self.dict.doc2bow(text.split(' '))
			if c == 5:
				print type(vector)
			self.f_corp.write(str(vector) + "\n")

			
		print "Corpus created, and written onto .braincorpus"
		
	def clean_text(self, force_clean = False): #generate cleaned text
		print "Attempting to clean text..."
		if os.path.exists('.braindump'):
			if not force_clean:
				pass
			else: 
				"Forced to clean text."
				self.cleaner()
		else:
				self.cleaner()
	def cleaner(self):
		self.f_text = open('.braindump', 'wb')
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


if __name__ == '__main__':
	#print removestopwords('This is a placeholder sentence, because I\'m bored.')
	main()
