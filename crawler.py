import os, sys, time, datetime
import twitter
import json
from config import ck, cs, ot, ots
from boto.s3.connection import S3Connection
try:
	from config import boto_access, boto_secret
except:
	pass
#import io
import bz2 #file compression

RAW_FILE = 'raw'
RAW_COMPRESSED_FILE = 'raw_compressed'
DECOMPRESSED_FILE = 'raw_decompressed'
BUCKET_NAME = 'twitter-deepthought'
def oauth_login(): #authenticate w twitter API
	CONSUMER_KEY = ck
	CONSUMER_SECRET = cs
	OAUTH_TOKEN = ot
	OAUTH_TOKEN_SECRET = ots

	auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
	twitter_api = twitter.Twitter(auth=auth)
	return twitter_api

""" ---- DO THINGS ---- """
def crawl():
	start_time = int(time.time()) #seconds from epoch
	twitter_api = oauth_login()
	print twitter_api
	twitter_stream = twitter.TwitterStream(auth=twitter_api.auth)
	stream = twitter_stream.statuses.sample(language = 'en')
	#c = 2 #placeholder, just to make sure the streaming ends for debugging
	outfile = open(RAW_FILE,'w') #file is opened for its lifetime as it is a better practice
	outcompress = bz2.BZ2Compressor()
	datetime_ = str(datetime.datetime.now())[:-7]
	outcompressfile = open('raw_compressed','w')
	for tweet in stream:
		#c -= 1
		current_time = int(time.time()) - start_time
		current_datetime = str(datetime.datetime.now())[:-7]
		outcompress.compress(json.dumps(tweet) + '\n\n')

		#print json.dumps(tweet, indent = 1)
		print current_time
		
		json.dump(tweet,outfile)
		try: 
			tweet['text'].encode('ascii', 'ignore')
		except:
			pass
		outfile.write('\n')
		if current_time > 2:
			outcompressfile.write(outcompress.flush())
			#print len(repr(outcompress.flush()))2
			break
def read():
	f = open(DECOMPRESSED_FILE ,'w')
	#for line in f:
	#	print line
	
	indecompress = bz2.BZ2Decompressor()
	compressedfile = open(RAW_COMPRESSED_FILE, 'r')
	for line in compressedfile:
		#print len(line)
		_text = indecompress.decompress(line)

		if '\n' in _text:
			print "!!!"
			f.write(str(_text.split('\n')))

def boto_save():
	conn = S3Connection(boto_access, boto_secret)
def boto_init():
	conn = S3Connection(boto_access, boto_secret)
	bucket = conn.create_bucket(BUCKET_NAME)
if __name__ == "__main__":
	crawl()	
	#read()