"""This module analyses tweets.csv and tps.csv files"""

from __future__ import division
import json
import collections
import csv
import os
import logging

from nltk.corpus import stopwords

from deepthought import config
from langprocess import LanguageProcesser

class Analyser(object):
    """Analyses tweets.csv and tps.csv and saves the result for later use

    Attributes:
        dir_path (str): The path of the directory containing the files to be analysed
        tweets_f (file): The tweets file to be analysed
        tps_f (file): The tps file to be analysed
    """

    def __init__(self):
        """Initializes the analyser"""
        # Initialize logger
        self.logger = logging.getLogger(__name__)

        self.dir_path = ""
        self.tweets_f = None
        self.tps_f = None

    def analyse(self, dir_path):
        """Starts analysis of the files

        Args:
            dir_path (str): The path of the directory containing the files to be analysed
        """
        self.dir_path = dir_path

        # Get the file path to the files
        tweets_f_path = os.path.join(self.dir_path, "tweets.csv")
        tps_f_path = os.path.join(self.dir_path, "tps.csv")

        # Check if the files actually exist
        if not (os.path.isfile(tweets_f_path) and os.path.isfile(tps_f_path)):
            # Else, raise and exception
            self.logger.error("Invalid file path")
            raise ValueError("Invalid file path")

        # Open the files for reading
        self.tweets_f = open(tweets_f_path, 'rb')
        self.tps_f = open(tps_f_path, 'rb')

        self.logger.info("Starting analysing of dir '" + self.dir_path + "'")

        # Generate dict containing the frequency of words
        self.gen_freq_dict()

        # Run spike detection function
        self.find_spikes()

        #Run keyword generation
        LanguageProceser(tweets_f_path)

        self.logger.info("Analysing done for '" + dir_path + "'")

    def gen_freq_dict(self):
        tweets_reader = csv.DictReader(self.tweets_f)
        tweets = ""

        # Concat each tweets' text
        for tweet in tweets_reader:
            tweets += json.loads(tweet['tweet'])['text'] + " "

        # Some processing of the tweets
        tweets = tweets.encode("utf-8", errors="replace")
        tweets = tweets.lower()
        tweets = tweets.split()
        stop_words = set(stopwords.words("english"))
        stop_words.update(["rt", "#"])
        tweets = [word for word in tweets if word not in stop_words]

        freq_dict = collections.Counter(tweets)
        search_fp = os.path.join(self.dir_path, "search.json")
        search_file = open(search_fp, "w")
        json.dump(freq_dict, search_file)


    def find_spikes(self):
        """Find if any spikes occurred with the given tps file

        This is done by calculating

        * `EMA (Exponential Moving Average) <http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average>`_,
        * the growth of EMA relative to previous EMAs,

        and then checking if the said growth exceeds an arbitrary threshold for it to be considered a spike.

        The growth of the EMA is calculated using the formula (current - previous) / previous.
        It is essentially a percentage increase of the EMA
        """
        # Load the TPS data from the file into a dict
        tps_reader = csv.reader(self.tps_f)
        tps_dict = dict(filter(None, tps_reader))

        # Convert the timestamps from strings to integers
        tps_dict.pop("timestamp", None)
        tps_dict = {int(float(k)): int(v) for k, v in tps_dict.items()}

        # Initialize variables for analysis
        ema = []  # A list is used for storing EMA as we want a stack-like behaviour
        growth = {}
        spikes = {}

        # Arbitrary values to check for spikes, defined in the config
        ema_length = config.ema_length
        growth_length = config.growth_length
        spike_threshold = config.spike_threshold

        # Calculate the constant 'k', to be used later when calculating the EMA
        k = 2 / (ema_length + 1)

        # Go through each tps value with a counter i (in seconds), which is used for calculating EMA
        for i, (timestamp, tps) in enumerate(tps_dict.iteritems()):
            # As calculating EMA requires a list of values, the first EMA value to be calculated
            # would be when there are enough values to calculate the EMA, which is defined by ema_length
            # This checks if the length has been reached (when i equals the length)
            if i == ema_length:
                # The first EMA value is the average of the list of values
                avg = sum(tps_dict.values()[:ema_length]) / ema_length
                ema.append({timestamp: avg})
            elif i > ema_length:  # For subsequent EMA values, a different formula is used
                # Get the previous EMA value (i-1)
                last_ema = ema[-1].values()[0]

                # Use the EMA formula to calculate the EMA
                current_ema = tps * k + last_ema * (1 - k)
                ema.append({timestamp: current_ema})

            # The growth of the EMA is calculated relative to previous values of EMA
            # Thus, this checks if there are enough EMA values to calculate this growth
            # (When there are <growth_length> number of EMA values)
            if i >= ema_length + growth_length:
                # Get the current EMA value, which is also the most recent value in the stack
                current_ema = ema[-1].values()[0]

                # Get the EMA value <growth_length> seconds before
                prev_ema = ema[-(1 + growth_length)].values()[0]

                # Calculates the growth with the formula (current-previous)/previous
                # Which is the percentage increase in EMA
                current_growth = (current_ema - prev_ema) / prev_ema
                growth[timestamp] = current_growth

                # Check if the growth has exceeded the threshold for it to be considered a spike
                if current_growth >= spike_threshold:
                    self.logger.info("Spike found at " + str(timestamp))

                    # Add timestamp and contents of the spike to the list
                    spikes[timestamp] = self.find_spike_contents(timestamp)

        stats = {
            # Combine the stack(list) of dicts into one big OrderedDict
            "ema": collections.OrderedDict((k, v) for d in ema for (k, v) in d.items()),

            # Sort the dict of values and store it in an OrderedDict
            "growth": collections.OrderedDict(sorted(growth.items())),
            "spikes": collections.OrderedDict(sorted(spikes.items()))
        }

        # Dump the stats to their respective csv files
        for (stat, values) in stats.iteritems():
            with open(os.path.join(self.dir_path, stat + ".csv"), 'w') as f:
                field_names = ['timestamp', stat]
                writer = csv.DictWriter(f, fieldnames=field_names)
                writer.writeheader()
                for timestamp, value in values.iteritems():
                    writer.writerow({'timestamp': timestamp, stat: value})

    def find_spike_contents(self, timestamp):
        """Given the timestamp of a spike, find what the tweets contained during the spike

        Args:
            timestamp (int): Timestamp of the spike

        Returns:
            Returns the top 5 words used in tweets during the spike
        """
        # Open the tweets file for reading
        tweets_reader = csv.DictReader(self.tweets_f)

        # This is a string to store all the contents of tweets that happened during the spike
        spike_tweets_text = ""

        # Go through each tweet in the file
        for tweet in tweets_reader:
            # Get the timestamp of this tweet
            tweet_timestamp = int(float(tweet['timestamp']))

            # We check if this tweets falls into the range where the spike happened
            # spike_contents_sample_size is an arbitrary value that 'defines' the duration of a spike
            # If a tweet happen to fall within this duration, it interests us
            if timestamp <= tweet_timestamp <= timestamp + config.spike_contents_sample_size:
                tweet_text = json.loads(tweet['tweet'])['text']
                spike_tweets_text += tweet_text

            # If this tweet falls outside of the range where the spike happened,
            # we break out of the loop as any tweets that happened after the spike doesnt interest us
            elif tweet_timestamp > timestamp + config.spike_contents_sample_size:
                break

        # Using a prebuilt function, we find the top 5 words that the tweets contained
        word_frequency_list = collections.Counter(spike_tweets_text.split()).most_common()
        return word_frequency_list[:5]
