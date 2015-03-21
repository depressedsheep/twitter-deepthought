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
import mongostuff
from nltk.corpus import stopwords
stop = stopwords.words('english')
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

def removestopwords(text):
	return [i for i in sentence.split() if i not in stop]

