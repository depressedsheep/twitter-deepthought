from gensim import corpora, models, similarities
import os
import cPickle as pickle

class blobbify(object):
	def __init__(self, key):
		self.key = key

		self.dirs = {
		'dict':os.path.join(os.path.dirname(__file__) + '/../thinking/braindict', self.key + '.dict'),
		'dump':os.path.join(os.path.dirname(__file__) + '/../thinking/braindump', self.key)
		'corp':os.path.join(os.path.dirname(__file__) + '/../thinking/braincorp', self.key + '.mm')
		}

	def gen(self):
		self.f_dict = open(os.path.join(self.dirs['dict'], self.key),'rb+') #dictionary
		self.f_text = open(os.path.join(self.dirs['dump'], self.key),'rb+')
		self.dict = pickle.load(self.f_dict)
		corpora.MmCorpus.serialize(self.dirs['corp'], [self.dict.doc2bow(line.split(' ')) for line in self.f_text])
	
		print 'MMCorpus file created at ' + self.dirs['corp']
		self.f_text.close()
		self.f_dict.close()