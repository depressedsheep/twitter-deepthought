from gensim import corpora, models, similarities
import os
import cPickle as pickle
import uuid

class librarian(object):
	def __init__(self, key_list):
		print "Initialising dictionary creation, brain.dicter.librarian()..."
		print "Assuming file has been cleaned for stopwords."
		self.key_list = key_list
		self.fname = str(uuid.uuid4())
		self.dirs = {
		'dict':os.path.join(os.path.dirname(__file__) + '/../thinking/braindict',  self.fname),
		'dump':os.path.join(os.path.dirname(__file__) + '/../thinking/braindump', self.fname),
		'in':os.path.join(os.path.dirname(__file__) + '/../thinking/braindump'),
		'flib':os.path.join(os.path.dirname(__file__) + '/../thinking/hashbrowns')
		}
	def merge(self):
		self.f_text = open(self.dirs['dump'], 'w')
		for key in self.key_list:
			f = open(os.path.join(self.dirs['in'], key), 'r')
			for line in f:
				self.f_text.write(line)
		print "Files merged."
	
	def cookhash(self):
		food = {}
		cake = '-'.join(self.key_list) 
		food[cake] = self.fname
		if not os.path.exists(self.dirs['flib']):			
			print "Cooking a new hashbrown."
			self.merge()
			pickle.dump(food, open(self.dirs['flib'], 'w'))
		else:
			cfood = pickle.load(open(self.dirs['flib'], 'r'))
			print cfood
			if cake in cfood:
				print "Hashbrown already exists."
				pass
			else:
				print "Cooking a new hashbrown."
				cfood[cake] = self.fname
				pickle.dump(cfood, open(self.dirs['flib'], 'w'))
				self.merge()
		print "Hashbrown done."
		return self.fname


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
