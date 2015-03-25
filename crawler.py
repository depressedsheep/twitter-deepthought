import os
import time
import datetime
import json
import twitter
import gzip
import threading
import platform
import sys
from collections import deque
from boto.s3.connection import Location, S3Connection
from boto.s3.key import Key
from config import ck, cs, ot, ots, boto_access, boto_secret
from pyqtgraph.Qt import QtGui, QtCore
import numpy as np
import pyqtgraph as pg
CONSUMER_KEY = ck
CONSUMER_SECRET = cs
OAUTH_TOKEN = ot
OAUTH_TOKEN_SECRET = ots


def main():
    # Run crawler
    c = Crawler()

    try:
        c.start()
    except KeyboardInterrupt:
        c.stop()


def init_twitter_api():
    """ Initializes a Twitter API object """
    # Authenticate with twitter api
    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
    twitter_api = twitter.Twitter(auth=auth)
    return twitter_api


def upload_file(file_path, key, bucket_name='twitter-deepthought'):
    """ Uploads the specified file to Amazon S3 through Boto
    :param file_path: The file path
    """
    # Authenticate with Amazon S3
    conn = S3Connection(boto_access, boto_secret)

    # If specified bucket doesn't exist, create it
    # Else, simply access it
    if conn.lookup(bucket_name) is None:
        bucket = conn.create_bucket(bucket_name, location=Location.SAEast)
    else:
        bucket = conn.get_bucket(bucket_name)

    # Store the file with specified key
    k = Key(bucket)
    k.key = key
    try:
        k.set_contents_from_filename(file_path)
    except:
        pass
class plot(object):
	def __init__(self):
		app = QtGui.QApplication([])
		win = pg.GraphicsWindow(title="Twitter Crawler Plot")
		win.resize(1000, 600)
		win.setWindowTitle('SMA')
		pg.setConfigOptions(antialias = True)
		self.p = win.addPlot(title = "SMA")
		self.curve = self.p.plot(pen = 'y')
		self.data = np.random.normal(size = 1000)		
		self.ptr = 0
		t = threading.Thread(target = self.start_plot)
		t.daemon = True
		t.start()
		pg.QtGui.QApplication.exec_()
	def start_plot(self):
		while True:
			self.update_plot()
			time.sleep(1.0)
	def update_plot(self):
		self.curve.setData(np.random.normal(size = 1000))
		if self.ptr ==0:
			self.p.enableAutoRange('xy', False)
		self.ptr += 1

class Crawler(object):
    """ Accepts Twitter tweet stream and save them hourly

    Attributes:
        total_tweets        The total number of tweets the crawler has collected
        tweets_per_second   The number of tweets received in the previous second
        start_time          Time when the crawler started
        stream              The Twitter stream where the crawler will get the tweets from
        file                The current GzipFile the crawler is writing to (changes every hour)
        sma_tweets          Queue to calculate Smoothed Moving Average of number of tweets collected
    """

    total_tweets = 0
    tweets_per_second = 0
    start_time = datetime.time()
    stream = twitter.TwitterStream()
    file = gzip.GzipFile
    sma_tweets = deque()

    def __init__(self, sma_length=10):
        """ Initializes class attributes
        :param sma_length: Duration (in minutes) of SMA sample size
        """
        # Extends the SMA Tweets queue to accommodate the sample size
        self.sma_tweets.extend([0] * (60 * sma_length))

        # Initializes a Twitter Stream
        twitter_api = init_twitter_api()
        self.stream = twitter.TwitterStream(auth=twitter_api.auth) \
            .statuses.sample(language='en')

        # Initializes the file the crawler is going to write to
        self.file = gzip.open(time.strftime('%d-%m-%Y_%H') + '.json.gz', 'ab')

    def start(self):
        """ Main function to start the collection of tweets """
        # Mark start time
        self.start_time = datetime.datetime.now()

        # Initial call to print_status, after which it will loop every 1s
        self.print_status()
        # Initial call to update_sma, after which it will loop every 1s
        self.update_sma()

        # Iterate tweets
        try:
            for tweet in self.stream:
                # Update counters
                self.total_tweets += 1
                self.tweets_per_second += 1

                # Write tweet to file
                self.file.write(json.dumps(tweet) + '\r\n')

                # If the current file name is outdated
                if self.file.name != time.strftime('%d-%m-%Y_%H') + '.json.gz':
                    self.change_file()
        except StopIteration:
            print "Stream stopped unexpectedly."
            self.stop()

    def stop(self):
        """ Do cleanup work """
        self.file.close()
        print 'Crawling stopped/interrupted'

    def update_sma(self):
        """ Update the SMA queue with the number of tweets
            received in the last second
        """
        # Call update_sma again 1s from now
        t = threading.Timer(1.0, self.update_sma)
        t.daemon = True
        t.start()
        #g = plot()
        # Remove the oldest record and add the latest record
        self.sma_tweets.pop()
        self.sma_tweets.appendleft(self.tweets_per_second)

        # Reset counter
        self.tweets_per_second = 0

   
    def print_status(self):
        """ Print the current status of the crawler to the terminal every second
        """
        # Call print_status again 1s from now
        t = threading.Timer(1.0, self.print_status)
        t.daemon = True
        t.start()

        # Attempt to clear the terminal
        os_name = platform.system()
      	""" if os_name == "Windows":
            os.system('cls')
        elif os_name == "Linux":
            os.system('clear')
        else:
            print "\n" * 100"""

        # Calculates some key statistics
        elapsed_time = datetime.datetime.now() - self.start_time
        if elapsed_time.total_seconds() > len(self.sma_tweets):
            sma = sum(self.sma_tweets) / float(np.count_nonzero(self.sma_tweets))
        else:
            sma = 0

        status = {
            'total_tweets': self.total_tweets,
            'duration': int(elapsed_time.total_seconds()),
            'sma': sma,
            'file_path': self.file.name,
            'file_size': os.path.getsize(self.file.name)
        }

        print status


    def change_file(self):
        """ Change the file the crawler is writing to
            and starts processing previous hour's file
        """
        if self.file.name != time.strftime('%d-%m-%Y_%H') + '.json.gz':
            # Sets the key of this file
            key = self.file.name.split('.')[0]

            # Starts the upload
            t = threading.Thread(target=upload_file, args=(self.file.name, key))
            t.daemon = True
            t.start()

            # Change file
            self.file.close()
            self.file = gzip.open(time.strftime('%d-%m-%Y_%H') + '.json.gz', 'ab')


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
    
if __name__ == '__main__':
	t = threading.Thread(target = main)
	t.daemon = True
	t.start()
	g = plot()


