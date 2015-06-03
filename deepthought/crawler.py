import os
import time
import datetime
import json
import twitter
import bz2
import threading
import logging
import csv
import socket
import shutil
import collections
import helpers
import config

module_logger = logging.getLogger(__name__)


def init_twitter_api():
    """ Initializes a Twitter API object """
    module_logger.debug("Initializing Twitter API")
    # Authenticate with twitter api
    auth = twitter.oauth.OAuth(config.ot, config.ots, config.ck, config.cs)
    twitter_api = twitter.Twitter(auth=auth)
    return twitter_api


class Crawler(object):
    """ Accepts Twitter tweet stream and save them hourly

    Attributes:
        total_tweets        The total number of tweets the crawler has collected
        tps                 Tweets Per Second
        start_time          Time when the crawler started
        stream              The Twitter stream where the crawler will get the tweets from
        dir                 The current directory where the crawler is writing to
        file_queue          The shared queue with the Spike thread for analysing files
        socket              The UDP socket used to send the current crawler status
        status              The current status of the crawler
        files               A dict of files to be used in the crawler
        writers             A dict of CSV Writers to be used in the crawler
        logger              Logger used for logging
    """

    total_tweets = 0
    tps = 0
    start_time = datetime.time()
    stream = twitter.TwitterStream()
    dir = ""
    file_queue = None
    socket = None
    status = {}
    files = {}
    writers = {}
    logger = None

    def __init__(self):
        """ Initializes class attributes
        """
        # Initialize logging
        self.logger = logging.getLogger(__name__)

        # Initializes a Twitter Stream
        twitter_api = init_twitter_api()
        self.logger.debug("Initializing Twitter Stream")
        self.stream = twitter.TwitterStream(auth=twitter_api.auth) \
            .statuses.sample(language='en')

        # Initializes the directory the crawler is going to write to
        self.init_dir()

    def __del__(self):
        """ Do cleanup work
        """
        self.logger.warn('Crawling stopped/interrupted')
        # Close the files
        for (file_name, file_handle) in self.files.iteritems():
            file_handle.close()
        self.socket.close()


    def start(self, file_queue=None):
        """
        Main function to start the collection of tweets
        :param file_queue: Shared queue between Crawler thread and Spike thread of files to be analysed
        """
        self.logger.info("Started crawling")

        # Initialize shared queue
        self.file_queue = file_queue

        # Mark start time
        self.start_time = datetime.datetime.now()

        # Initial call to update status
        self.update_status()

        # Start status UDP server
        udp_t = threading.Thread(target=self.send_status)
        udp_t.start()
        udp_t.name = "UDP Server Thread"

        # Iterate tweets
        try:
            for tweet in self.stream:
                # Update counters
                self.total_tweets += 1
                self.tps += 1

                # Write tweet to file
                timestamp = time.time()
                self.files["tweets"].writerow({
                    'timestamp': timestamp,
                    'tweet': json.dumps(tweet)
                })

                # Check if it's time to change dir
                if self.dir != time.strftime('%d-%m-%Y_%H'):
                    self.change_dir()
        except StopIteration:
            self.logger.error("Twitter stream stopped unexpectedly")

    def update_status(self):
        """ Update the crawler's status and calculate the tps
        """
        if threading is None:
            return

        # Update status every second
        t = threading.Timer(1, self.update_status)
        t.start()
        t.name = "Crawler status thread"

        # Calculate time elapsed
        elapsed_time = datetime.datetime.now() - self.start_time

        self.status = {
            'duration': int(elapsed_time.total_seconds()),
            'total_tweets': self.total_tweets,
            'tps': self.tps,
            'dir': self.dir
        }

        # Update tps file
        timestamp = str(time.time())
        self.files["tps"].writerow({
            'timestamp': timestamp,
            'tps': self.tps
        })

        # Reset counter for tweets per second
        self.tps = 0

    def send_status(self):
        """
        Reply any queries for the status of the crawler
        """
        # Initializes the UDP socket
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.logger.debug("Socket created")
        except socket.error, msg:
            self.logger.error("Failed to create socket. Message: " + msg[1])
            raise

        # Bind socket to host and port
        try:
            self.socket.bind((config.udp_host, config.udp_port))
        except socket.error, msg:
            self.logger.error("Bind failed. Message: " + msg[1])
            raise
        self.logger.debug("Socket bind completed.")

        while True:
            addr = self.socket.recvfrom(1024)[1]
            reply = json.dumps(self.status)
            self.socket.sendto(reply, addr)

    def change_dir(self):
        """ Change the dir the crawler is writing to and starts processing previous hour's dir
        """
        self.logger.info("Changing dir from '" + self.dir + "' to '" + time.strftime('%d-%m-%Y_%H') + "'")

        # Close the old files
        for (file_name, file_handle) in self.files.iteritems():
            file_handle.close()

        # Add the file path of the old dir to the queue for analysing
        if self.file_queue is not None:
            self.file_queue.put(self.dir)

        # Init new dir
        self.init_dir()

    def init_dir(self):
        """ Initializes the directory the crawler is going to write to
        """
        self.dir = time.strftime('%d-%m-%Y_%H')
        self.logger.debug("Initializing dir '" + self.dir + "'")

        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        self.files["tweets"] = open(os.path.join(self.dir, "tweets.csv"), 'wb', 0)
        writer = csv.DictWriter(self.files["tweets"], fieldnames=['timestamp', 'tweet'])
        writer.writeheader()
        self.writers["tweets"] = writer

        self.files["tps"] = open(os.path.join(self.dir, "tps.csv"), 'wb', 0)
        writer = csv.DictWriter(self.files["tps"], fieldnames=['timestamp', 'tps'])
        writer.writeheader()
        self.writers["tps"] = writer