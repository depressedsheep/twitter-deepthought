import os
import time
import datetime
import json
import twitter
import gzip
import threading
import logging
import pprint
import shutil
import helpers
from config import ck, cs, ot, ots, public_dir


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p')

    # Run crawler
    c = Crawler()
    try:
        c.start()
    except KeyboardInterrupt:
        c.stop()


def init_twitter_api():
    """ Initializes a Twitter API object """
    # Authenticate with twitter api
    auth = twitter.oauth.OAuth(ot, ots, ck, cs)
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
        tweets_file         The current file where tweets are being stored
        tps_file            The current file where Tweets Per Second are being recorded
    """

    total_tweets = 0
    tps = 0
    start_time = datetime.time()
    stream = twitter.TwitterStream()
    dir = ""
    tweets_file = gzip.GzipFile
    tps_file = gzip.GzipFile

    def __init__(self):
        """ Initializes class attributes
        """
        logging.info("Initializing twitter stream")
        # Initializes a Twitter Stream
        twitter_api = init_twitter_api()
        self.stream = twitter.TwitterStream(auth=twitter_api.auth) \
            .statuses.sample(language='en')

        # Initializes the directory the crawler is going to write to
        self.init_dir()

    def start(self):
        """ Main function to start the collection of tweets """
        logging.info("Starting crawling")

        # Mark start time
        self.start_time = datetime.datetime.now()

        # Initial call to update status
        self.update_status()

        # Iterate tweets
        try:
            for tweet in self.stream:
                # Update counters
                self.total_tweets += 1
                self.tps += 1

                # Write tweet to file
                timestamp = str(time.time())
                self.tweets_file.write('"' + timestamp + '":' + json.dumps(tweet) + ',')

                # If the current directory name is outdated
                if self.dir != time.strftime('%d-%m-%Y_%H'):
                    self.change_dir()
        except StopIteration:
            logging.error("Stream stopped unexpectedly")
            self.stop()

    def stop(self):
        """ Do cleanup work
        """
        logging.warn('Crawling stopped/interrupted')
        self.tweets_file.close()
        self.tps_file.close()

    def update_status(self):
        """ Log the current status of the crawler and sends status to frontend
        """
        if threading is None:
            return

        # Update status every second
        t = threading.Timer(1, self.update_status)
        t.daemon = True
        t.start()

        # Calculate time elapsed
        elapsed_time = datetime.datetime.now() - self.start_time

        status = {
            'duration': int(elapsed_time.total_seconds()),
            'total_tweets': self.total_tweets,
            'tps': self.tps,
            'dir': self.dir,
            'tweets_file_size': os.path.getsize(self.tweets_file.name),
        }

        # Logs to console
        logging.debug("\n" + pprint.pformat(status) + "\n")

        # Update status to front end
        with open(public_dir + "status.json", 'w') as f:
            f.write(json.dumps(status))

        # Update tps file
        timestamp = str(time.time())
        self.tps_file.write("\"" + timestamp + "\":" + str(self.tps) + ',')

        # Reset counter for tweets per second
        self.tps = 0

    def change_dir(self):
        """ Change the dir the crawler is writing to
            and starts processing previous hour's dir
        """
        if self.dir != time.strftime('%d-%m-%Y_%H'):
            logging.info("Changing dir from " + self.dir + " to " + time.strftime('%d-%m-%Y_%H'))

            # Close the old files to allow processing
            self.tweets_file.close()
            self.tps_file.close()

            # Starts the upload
            t = threading.Thread(target=self.process_dir, args=(self.dir,))
            t.daemon = True
            t.start()

            # Change dir
            self.init_dir()

    def init_dir(self):
        """ Initializes the directory the crawler is going to write to
        """

        self.dir = time.strftime('%d-%m-%Y_%H')
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        self.tweets_file = open(self.dir + '/tweets.json', 'ab')
        self.tps_file = open(self.dir + '/tps.json', 'ab')

    @staticmethod
    def process_dir(dir):
        """
        Given a dir, compress and process all its files, and then
        upload and delete it
        :param dir: The name of dir to process
        :return:
        """
        logging.info("Processing dir " + dir)
        # Loop through each file in the directory
        for root, dirs, files in os.walk(dir):
            for name in files:
                file_path = os.path.join(root, name)

                # Read the contents of the file
                original_f = open(file_path)
                contents = original_f.read()
                original_f.close()

                # Process JSON files
                if ".json" in name:
                    contents = '{' + contents[:-1] + '}'

                # Compress and write the contents to a new file
                compressed_f = gzip.open(file_path + '.gz', 'wb')
                compressed_f.write(contents)
                compressed_f.close()

                # Remove the old, uncompressed file
                os.remove(file_path)

        # Upload the directory
        helpers.upload_dir(dir)

        # Delete the directory after upload, to save space
        shutil.rmtree(dir)


if __name__ == '__main__':
    main()