from gensim import corpora, models, similarities
import os
import cPickle as pickle
class librarian(object):
	def __init__(self, key):
		print "Initialising dictionary creation, brain.dicter.librarian()..."
		print "Assuming file has been cleaned for stopwords."
		self.key = key

		self.dirs = {
		'dict':os.path.join(os.path.dirname(__file__) + '/../thinking/braindict', self.key + '.dict'),
		'dump':os.path.join(os.path.dirname(__file__) + '/../thinking/braindump', self.key)	
		}
		
	def gen(self):
		self.f_dict = open(self.dirs['dict'],'w')
		self.f_text = open(self.dirs['dump'],'r')

		self.dict = corpora.Dictionary(line[:-1].lower().split() for line in self.f_text)
		once_ids = [tokenid for tokenid,docfreq in self.dict.iteritems() if docfreq == 1]
		self.dict.filter_tokens(once_ids)
		self.dict.compactify()

		pickle.dump(self.dict, self.f_dict) #this is a dump of the dictionary

		self.f_text.close()
		self.f_dict.close()
		
		print "Dict file created at " + self.dirs['dict']
