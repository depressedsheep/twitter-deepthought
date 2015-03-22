import os, sys, time, datetime
import twitter
import json
import mongostuff
from config import ck, cs, ot, ots
import botostuff
#import io
import bz2
import atexit
import threading
import platform
import time
from collections import deque
from shutil import copyfileobj
import numpy as np
import math
#RAW_FILE = 'raw'
#RAW_COMPRESSED_FILE = 'raw_compressed.bz2'
#DECOMPRESSED_FILE = 'raw_decompressed'

def r(x,n):
	if int(x) == 0:
		return 0
	else:
		return round(x, int(n - math.ceil(math.log10(abs(x)))))
def main():
	c = Crawler()
	try:
		c.start()
	except KeyboardInterrupt:
		c.stop()

class Crawler(object):
	def __init__(self):
		self.num_tweets = 0
		self.num_tweets_ = 0
		self.sma_tweets = deque([0] * 600)
		try:
			os.remove('temp')
			os.remove('temp1')
		except:
			pass
		self.start_time = int(time.time())
		self.start_date = str(datetime.datetime.now())[:-7]
		self.tfpswitch = 'f'
		self.compressedfp = str(datetime.datetime.now())[:13].replace(' ', '_') + '.bz2'
		self.f = open('temp', 'wb')
		#self.g = open('temp1','wb')
		self.hour = datetime.datetime.now().hour
		self.second = datetime.datetime.now().second
		twitter_api = oauth_login()
		twitter_stream = twitter.TwitterStream(auth=twitter_api.auth)
		self.stream = twitter_stream.statuses.sample(language = 'en')
	def start(self):
		self.print_status()
		for tweet in self.stream:
			self.num_tweets += 1
			self.num_tweets_ += 1
			self.c_hour = datetime.datetime.now().hour
			self.c_second = datetime.datetime.now().second
			#change hour
			if self.c_second != self.second:
				self.second = self.c_second
				self.sma_tweets.pop()
				self.sma_tweets.appendleft(self.num_tweets_)
				self.num_tweets_ = 0
			if self.c_hour != self.hour:
			#if self.c_second != self.second:
				if self.tfpswitch == 'f':
					self.f.close()
					try:
						os.remove('temp1')
					except:
						pass
					self.tfpswitch = 'g'
					self.g = open('temp1','wb')
				else:
					self.tfpswitch = 'f'
					try:
						os.remove('temp')
					except:
						pass
					self.g.close()
					self.f = open('temp', 'wb')
				self.update = threading.Thread(target=self.save_lasthour, args=(self.tfpswitch,))
				self.update.daemon = True
				self.update.start()
				
				#self.second = self.c_second
				self.hour = self.c_hour
			else:
				if self.tfpswitch == 'f':
					self.f.write(json.dumps(tweet))
				else:
					self.g.write(json.dumps(tweet))
				
	def save_lasthour(self, fg):
		if fg == 'g':
			print "attempting to compress"			
			with open('temp', 'rb') as input:
				with bz2.BZ2File(os.path.join('compressed-tweets', self.compressedfp), 'wb', compresslevel = 9) as output:
					copyfileobj(input, output)
			
			botostuff.save(str(datetime.datetime.now())[:13].replace(' '
				, '_')), os.path.join('compressed-tweets', self.compressedfp)
			self.compressedfp = str(datetime.datetime.now())[:13].replace(' '
				, '_') + '.bz2'
			return

		else:
			print "attempting to compress"			
			with open('temp1', 'rb') as input:
				with bz2.BZ2File(os.path.join('compressed-tweets', self.compressedfp), 'wb', compresslevel = 9) as output:
					copyfileobj(input, output)
			self.compressedfp = str(datetime.datetime.now())[:13].replace(' ', '_') + '.bz2'			
			return
	def stop(self):
		print 'Crawling stopped/interrupted'
		self.f.close()
		try:
			self.g.close()
		except:
			pass
	def print_status(self):
				# Print status every 1.0 second
		self.t = threading.Timer(1.0, self.print_status)
		self.t.daemon = True
		self.t.start()
		
		# Attempt to clear the terminal
		os_name = platform.system()
		if os_name == "Windows": os.system('cls')
		elif os_name == "Linux": os.system('clear')
		else: print "something wrong"
		
		print "Current number of tweets: " + str(self.num_tweets)
		#print "Current file size: " + str(os.path.getsize(filepath)/(1000000)) + "mb"
		print "Time elapsed: " + str(int(time.time()-self.start_time)/(60*60)) + "h " + str((int(time.time() - self.start_time)%3600)/60) + "m"
		try:
			print "Moving tweet frequency average (SMA) for past 10 minutes: " + str(sum(self.sma_tweets)/float(np.count_nonzero(self.sma_tweets)))

		except:
			print "Moving tweet frequency average (SMA) for past 10 minutes: 0"
		print "\nCtrl+C to stop crawling"


def oauth_login(): #authenticate w twitter API
	CONSUMER_KEY = ck
	CONSUMER_SECRET = cs
	OAUTH_TOKEN = ot
	OAUTH_TOKEN_SECRET = ots

	auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
	twitter_api = twitter.Twitter(auth=auth)
	return twitter_api

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

