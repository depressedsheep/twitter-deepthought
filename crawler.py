import os, sys, time, datetime
import twitter
import json
import mongostuff
from config import ck, cs, ot, ots
from boto.s3.connection import S3Connection
from boto.s3.key import Key as botoKey
try:
	from config import boto_access, boto_secret
except:
	pass
#import io
import bz2 #file compression
#
#RAW_FILE = 'raw'
#RAW_COMPRESSED_FILE = 'raw_compressed.bz2'
#DECOMPRESSED_FILE = 'raw_decompressed'
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
def crawl(_duration):
	start_time = int(time.time()) #seconds from epoch
	twitter_api = oauth_login()
	print twitter_api
	twitter_stream = twitter.TwitterStream(auth=twitter_api.auth)
	stream = twitter_stream.statuses.sample(language = 'en')
	#c = 2 #placeholder, just to make sure the streaming ends for debugging
	#outfile = open(RAW_FILE,'w') #file is opened for its lifetime as it is a better practice
	datetime_ = str(datetime.datetime.now())[:-7]
	if not os.path.exists('compressed-tweets'):
		os.makedirs('compressed-tweets')

	#outcompressfile = open(os.path.join('compressed-tweets',datetime_+'.bz2'),'w')
	#outcompress = bz2.BZ2Compressor()
	
	for tweet in stream:
		#c -= 1
		current_time = int(time.time()) - start_time
		current_datetime = str(datetime.datetime.now())[:-7]
		mongostuff.save(tweet,str(datetime.datetime.now())[:13].replace(' ',''), 'universe')
		#outcompress.compress(json.dumps(tweet) + '\n')
		#print json.dumps(tweet, indent = 1)
		#print current_time
		
		#json.dump(tweet,outfile)
		#try: 
		#	print tweet['text'].encode('ascii', 'ignore')
		#except:
		#	pass
		#outfile.write('\n')
		if current_time > _duration:
			#outcompressfile.write(outcompress.flush())
			#print len(repr(outcompress.flush()))2
			break

def decompress(filedate):
	if not os.path.exists('decompressed-tweets'):
		os.makedirs('decompressed-tweets')
	f = open(os.path.join('decompressed-tweets', filedate + '.bz2') ,'w')
	
	indecompress = bz2.BZ2Decompressor()
	compressedfile = open(os.path.join('raw-tweets', filedate + '.bz2') ,'r')
	for line in compressedfile:
		f.write(indecompress.decompress(line))
def trends():
	datetime_ = str(datetime.datetime.now())[:-7]
	if not os.path.exists('trends'):
		os.makedirs('trends')
	f = open(os.path.join('trends',datetime_ + ' ' + 'sg-trends'), 'w')
	def twitter_trends(twitter_api, woe_id):
		return twitter_api.trends.place(_id = woe_id)
	twitter_api = oauth_login()
	SG_WOE_ID = 1062617
	sg_trends = twitter_trends(twitter_api, SG_WOE_ID)
	f.write(json.dumps(sg_trends, indent = 1))

def boto_save(key, filename):
	conn = S3Connection(boto_access, boto_secret)
	try:
		bucket = conn.create_bucket(BUCKET_NAME, location =Location.SAEast)
	except:
		bucket = conn.get_bucket(BUCKET_NAME)
	k = Key(bucket)
	k.key = key
	k.set_contents_from_filename(filename)

if __name__ == "__main__":
	crawl(15)
	#print load_from_mongo('2015-03-2022', 'universe')
	#decompress()
	#trends() 'universe')