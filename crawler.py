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
def main():
	c = Crawler()
	try:
		c.start()
	except KeyboardInterrupt:
		c.stop()


def oauth_login(): #authenticate w twitter API
	CONSUMER_KEY = ck
	CONSUMER_SECRET = cs
	OAUTH_TOKEN = ot
	OAUTH_TOKEN_SECRET = ots

	auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
	twitter_api = twitter.Twitter(auth=auth)
	return twitter_api

class Crawler(object):
	
	def __init__(self):
		self.start_time = int(time())
		self.num_tweets = 0
		self.f = gzip.open(FILE_PATH, 'wb')
		
		twitter_api = oauth_login()
		twitter_stream = twitter.TwitterStream(auth=twitter_api.auth)
		self.stream = twitter_stream.statuses.sample(language = 'en')
		
	def start(self):
		self.print_status()
		
		for tweet in self.stream:
			self.num_tweets += 1
			self.f.write(json.dumps(tweet))
	
	def stop(self):
		print 'Crawling stopped/interrupted'
		self.f.close()
		
	def print_status(self):
		# Print status every 1.0 second
		self.t = threading.Timer(1.0, self.print_status)
		self.t.daemon = True
		self.t.start()
		
		# Attempt to clear the terminal
		os_name = platform.system()
		if os_name == "Windows": os.system('cls')
		elif os_name == "Linux": os.system('clear')
		else: print "\n"*100
		
		print "Current number of tweets: " + str(self.num_tweets)
		print "Current file size: " + str(os.path.getsize(FILE_PATH)/(1000000)) + "mb"
		print "Time elapsed: " + str(int(time()-self.start_time)/(60*60)) + "h"
		print "Avg tweets per second: " + str(self.num_tweets/int(time()-self.start_time))
		print "\n Ctrl+C to stop crawling"

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

#this function is purely for convenience, you know.
def time_change(t, units):
	# use: time_change(1, 'minute')
	# returns 60
	if units == 'minute':
		return t * 60
	elif units == 'hour':
		return t * 60 * 60
	elif units == 'day':
		return t * 60 * 60 * 24
	else:
		raise ValueError("Typo in the units?")

if __name__ == "__main__":
	main()
	#print load_from_mongo('2015-03-2022', 'universe')
	#decompress()
	#trends() 'universe')

