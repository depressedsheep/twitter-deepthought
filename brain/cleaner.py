from nltk.corpus import stopwords
import os
import json
import re
import base64

stop = stopwords.words('english')

class broom(object):
	def __init__(self, f, key):
		print "Initialising brain.cleaner.broom()..."
		print "This module assumes that you've already loaded the .gz file."
		self.key = key
		self.f = f
		self.dirs = {
		'dump':os.path.join('/../thinking/braindump', self.key),
		'raw':os.path.join('')
		}
		self.f_text = open(os.path.dirname(__file__) + self.dirs['dump'], 'w')
		self.t_stop = ['rt', '#','http', '@']		
	def sweep(self):
		for tweet in self.f:
			tweet = json.loads(tweet)
			text = tweet['text']
			text = self.clean(text)
			self.f_text.write(' '.join(text).encode('ascii','ignore') + '\n')
		self.f_text.close()
		print "Dump file for cleaned text created at " + self.dirs['dump']
	def clean(self,text):
		tl = unicode(text.lower()).split(' ')
		tl = self.strip_emojis(tl)
		tl = filter(lambda w: (not w in stop), tl)
		tl = map(self.strip_escape, tl)
		tl = filter(self.strip_others, tl)
		return tl
	def strip_emojis(self, tl):
		myre = re.compile(u'['u'\U0001f300-\U0001ffff'u'\U0001f600-\U0001f64f'u'\U0001f680-\U0001f6ff'u'\u2600-\u26ff\u2700-\u27bf]+', re.UNICODE)
		return myre.sub('', ' '.join(tl)).split(' ')
	def strip_escape(self,text):
		while True:
			if text[:1] == '\n':
				text = text[1:]
			else:
				break
		return text
	def strip_others(self, text):
		for a in self.t_stop:
			if a in text:
				return False
		return True