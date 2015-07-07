"""This module collects tweets and saves them for analysis later"""

import os
import time
import datetime
import json
import logging
import csv
import threading
import urllib2
import traceback
import shutil

import twitter

from deepthought import config


module_logger = logging.getLogger(__name__)


def init_twitter_api():
    """ Helper function to connect to the Twitter API

    Returns:
        twitter_api (twitter.TwitterStream): Twitter API object
    """
    module_logger.debug("Initializing Twitter API")
    # Authenticate with twitter api
    auth = twitter.oauth.OAuth(config.ot, config.ots, config.ck, config.cs)
    twitter_api = twitter.Twitter(auth=auth)
    return twitter_api


class Crawler(threading.Thread):
    """ Accepts Twitter stream and saves tweets hourly

    Attributes:
        total_tweets (int): The total number of tweets the crawler has collected
        tps (int): Variable to calculate Tweets Per Second
        start_time (datetime): Time when the crawler started
        stream (twitter.TwitterStream): The Twitter stream where the crawler will get the tweets from
        dir (str): The current directory where the crawler is writing to
        status (dict): The current status of the crawler
        files (dict): A dict of files to be used in the crawler, namely tweets.csv and tps.csv
        writers (dict): A dict of CSV Writers to be used in the crawler
        logger (logging.Logger): Logger used for logging
        stopped (bool): Boolean value indicating if crawler has stopped
    """

    def __init__(self):
        """Initializes crawler"""
        self.total_tweets = 0
        self.tps = 0
        self.start_time = None
        self.stream = None
        self.dir = ""
        self.status = {}
        self.files = {}
        self.writers = {}
        self.stopped = False

        # Call thread constructor
        super(Crawler, self).__init__()

        # Initialize logging
        self.logger = logging.getLogger(__name__)

    def run(self):
        """Main function for the collection of tweets

        It establishes a connection to the Twitter Stream and iterate over each tweet, appending the timestamp and tweet
        to the tweet.csv file. It also checks if it is time to change directory.
        """

        self.logger.warn("Crawler started")

        # Initializes a Twitter Stream
        twitter_api = init_twitter_api()
        self.logger.debug("Initializing Twitter Stream")
        try:
            self.stream = twitter.TwitterStream(auth=twitter_api.auth) \
                .statuses.sample(language='en')
        except urllib2.URLError:  # This exception is raised when there is no internet
            self.logger.error("Unable to initialize Twitter stream, please check your internet connection")
            raise

        # Initializes the directory the crawler is going to write to
        self.init_dir()

        # Mark start time
        self.start_time = datetime.datetime.now()

        # Start updating status every second from now
        self.update_status()

        # Iterate the tweets in the stream
        try:
            for tweet in self.stream:
                # If the crawler has been stopped, break out of the loop
                if self.stopped:
                    break

                # Update counters
                self.total_tweets += 1
                self.tps += 1

                # Append tweet to csv file
                timestamp = time.time()
                try:
                    self.writers["tweets"].writerow({
                        'timestamp': timestamp,
                        'tweet': json.dumps(tweet)
                    })
                except ValueError:
                    self.logger.error("Failed to write to tweets.csv")
                    raise

                # Check if it is time to change dir, which happens every hour
                if self.dir != self.get_curr_hour():
                    self.change_dir()
        except StopIteration:
            self.logger.error("Twitter stream stopped unexpectedly")

    def stop(self):
        """Stops the crawler

        It ensures that all file I/Os stops and closes the file handlers to prevent corrupted data
        """
        self.logger.warn('Crawling stopped/interrupted')

        # Set stopped to True
        self.stopped = True

        # Ensure that no more writing will occur
        time.sleep(1.5)

        # Close the files
        for (file_name, file_handle) in self.files.iteritems():
            file_handle.close()

    def update_status(self):
        """Update the crawler's status and calculates the TPS

        Note:
            The tps is calculated using a counter, which is incremented every time a tweet is processed.
            Every second, the value of the counter is noted and is reset to 0.
        """
        # If the crawler has been stopped, break
        if self.stopped:
            return

        # Call this function again one second from now
        t = threading.Timer(1, self.update_status)
        t.daemon = True
        t.start()

        # Calculate time elapsed
        elapsed_time = datetime.datetime.now() - self.start_time

        self.status = {
            'duration': int(elapsed_time.total_seconds()),
            'total_tweets': self.total_tweets,
            'tps': self.tps,
            'dir': self.dir
        }

        # Append to the tps file
        timestamp = str(time.time())
        try:
            self.writers["tps"].writerow({
                'timestamp': timestamp,
                'tps': self.tps
            })
        except ValueError:
            self.logger.error("Failed to write to tps.csv")
            raise

        # Reset tps counter
        self.tps = 0

    def change_dir(self):
        """Sends the old files to the Processor and initializes a new directory"""
        self.logger.info("Changing dir from '" + self.dir + "' to '" + self.get_curr_hour() + "'")

        # Close the old files
        for (file_name, file_handle) in self.files.iteritems():
            file_handle.close()

        # Init new dir
        self.init_dir()

    def init_dir(self):
        """Initializes a directory for the crawler

        It also opens tweets.csv and tps.csv in the directory for writing
        """
        self.dir = self.get_curr_hour()
        self.logger.debug("Initializing dir '" + self.dir + "'")

        # If directory already exists, delete it and make a new one
        if os.path.exists(self.dir):
            self.logger.warning("Dir '" + self.dir + "' already exists, deleting")
            shutil.rmtree(self.dir)

        os.makedirs(self.dir)

        # Creates and opens tweets.csv and tps.csv for writing
        # They will be closed when it's time to change dir or if the crawler has been stopped
        self.files["tweets"] = open(os.path.join(self.dir, "tweets.csv"), 'wb', 0)
        writer = csv.DictWriter(self.files["tweets"], fieldnames=['timestamp', 'tweet'])
        writer.writeheader()
        self.writers["tweets"] = writer

        self.files["tps"] = open(os.path.join(self.dir, "tps.csv"), 'wb', 0)
        writer = csv.DictWriter(self.files["tps"], fieldnames=['timestamp', 'tps'])
        writer.writeheader()
        self.writers["tps"] = writer

    @staticmethod
    def get_curr_hour():
        """Method that returns the current hour

        Returns:
            The current hour in the format of '%d-%m-%Y_%H'

        Note:
            The working dir is prepended
        """
        return os.path.join(config.working_dir, time.strftime('%d-%m-%Y_%H'))


def init_logging():
    log_formatter = logging.Formatter(
        fmt="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s", datefmt="%m/%d/%Y %I:%M %p")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)


if __name__ == "__main__":
    crawler = Crawler()
    init_logging()
    while True:
        try:
            crawler.start()
        except KeyboardInterrupt:
            break
        except:
            dump_file_path = time.strftime('%d-%m-%Y_%H_%M') + ".dump"
            div = "\n\n" + "-" * 50 + "\n\n"
            print div + "Fatal error occurred in crawler! Dumping stack trace to '" + dump_file_path + "'" + div
            traceback.print_exc(file=open(dump_file_path, 'wb'))
        else:
            break
