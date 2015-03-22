#
# Main analysis module for compressed files. Imports crawler.py and aims to define functions 
# to parse and analyse the tweets in various ways, most probably with gensim
# 
# Goals
# - TF-IDF vectors
# - Create a LSA model and update it at every time interval
# - Figure out how to run this concurrently with crawler.py
#
from crawler import decompress
from gensim import corpora, models, similarities
import logging
import os
import botostuff
from nltk.corpus import stopwords

stop = stopwords.words('english')
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

def removestopwords(text):
	text = text.encode('ascii', 'ignore')
	stoppedtext = [i.lower() for i in text.split() if i not in stop]
	return stoppedtext
#
# this part assumes loading from boto
def process(filename, initcorpora = False): #collection name (e.g. 2015-03-21_18)
	if initcorpora == True:
		



if __name__ == '__main__':
	#print removestopwords('This is a placeholder sentence, because I\'m bored.')
	process('2015-03-21_18', initcorpora = True)
