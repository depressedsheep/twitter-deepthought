from __future__ import division
import json
import collections
import csv
import os
import logging

import config


class SpikeDetector(object):
    """ Given a dir, find if any spikes occurred """

    def __init__(self):
        # Initialize logger
        self.logger = logging.getLogger(__name__)

    def find_spikes(self, dir_path):
        """
        Given a file path, attempt to find the spikes (if any) that occurred within that hour by calculating EMA,
        EMA growth and then checking if said growth exceeds the threshold
        :param file_path: Path to the dir where data is stored
        """
        self.logger.info("Starting spike detection of dir '" + dir_path + "'")

        # Load the TPS data from the file into the dict
        tps_f = open(os.path.join(dir_path, "tps.csv"), 'r')
        tps_reader = csv.reader(tps_f)
        tps_dict = dict(filter(None, tps_reader))

        # Convert the timestamps from strings to integers
        tps_dict.pop("timestamp", None)
        tps_dict = {int(float(k)): int(v) for k, v in tps_dict.items()}

        # Initialize variables for storing EMA values, growth values, and timestamps of spikes
        ema = []  # List is used for EMA as we want stack-like behaviour
        growth = {}
        spikes = {}

        # Get values to be used later from config
        ema_length = config.ema_length
        growth_length = config.growth_length
        spike_threshold = config.spike_threshold

        # Calculate variable k, to be used later to calculate EMA
        k = 2 / (ema_length + 1)

        # Loop through each tps
        for i, (timestamp, tps) in enumerate(tps_dict.iteritems()):

            # Calculate EMA
            if i == ema_length:  # If this is the first EMA value

                # EMA = the average of the tps values before this
                avg = sum(tps_dict.values()[:ema_length]) / ema_length

                # Add EMA value to list
                ema.append({timestamp: avg})

            elif i > ema_length:  # If this is not the first EMA value

                # Peek the top (previously inserted) EMA value in the stack
                last_ema = ema[-1].values()[0]

                # Use EMA formula to calculate EMA for this particular timestamp
                # Refer to http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
                current_ema = tps * k + last_ema * (1 - k)

                # Add EMA value to the list
                ema.append({timestamp: current_ema})

            # Calculates EMA growth relative to a certain duration prior
            if i >= ema_length + growth_length:  # If there are enough EMA values to calculate the growth

                # Get the current EMA value for this particular timestamp
                current_ema = ema[-1].values()[0]

                # Get the EMA value "growth_length" duration before
                prev_ema = ema[-(1 + growth_length)].values()[0]

                # Calculates the growth and add it to the dict
                current_growth = (current_ema - prev_ema) / prev_ema
                growth[timestamp] = current_growth

                # Check if growth exceeded the threshold to be considered a spike
                if current_growth >= spike_threshold:
                    self.logger.info("Spike found at " + str(timestamp))

                    # Add timestamp and contents of the spike to the list
                    spikes[timestamp] = self.find_spike_contents(timestamp, dir_path)

        data = {
            # Combine the list of dicts into one big OrderedDict
            "ema": collections.OrderedDict((k, v) for d in ema for (k, v) in d.items()),

            # Sort the dict of values and store it in an OrderedDict
            "growth": collections.OrderedDict(sorted(growth.items())),
            "spikes": collections.OrderedDict(sorted(spikes.items()))
        }

        # Dump the stats to their respective csv files
        for (stat, values) in data.iteritems():
            with open(os.path.join(dir_path, stat + ".csv"), 'w') as csv_file:
                field_names = ['timestamp', stat]
                writer = csv.DictWriter(csv_file, fieldnames=field_names)

                writer.writeheader()
                for timestamp, value in values.iteritems():
                    writer.writerow({'timestamp': timestamp, stat: value})

        self.logger.info("Spike detection done and recorded for '" + dir_path + "'")

    @staticmethod
    def find_spike_contents(timestamp, file_path):
        """
        Given the timestamp of the spike, find the contents of tweets during the spike
        :param timestamp: Timestamp when spike happened
        :param file_path: Path to dir where data is stored
        :return: Returns the top 5 words used in tweets during the spike
        """
        # Init the CSV reader for the tweets file
        tweets_f = open(os.path.join(file_path, "tweets.csv"))
        tweets_reader = csv.DictReader(tweets_f)

        # Init a empty list to store the tweets' texts
        spike_tweets_text = []

        # Go through each tweet in the file
        for tweet in tweets_reader:

            # Get the timestamp of the tweet
            tweet_timestamp = int(float(tweet['timestamp']))

            # Check if the tweet was tweeted during the spike time
            # spike_contents_sample_size is an arbitrary duration in seconds after the detected spike where
            # the tweets are relevant to the reason for the spike
            if timestamp <= tweet_timestamp <= timestamp + config.spike_contents_sample_size:

                # Add the tweet text to the list
                tweet_text = json.loads(tweet['tweet'])['text']
                spike_tweets_text.append(tweet_text)

            # Exit the for loop if we are done
            elif tweet_timestamp > timestamp + config.spike_contents_sample_size:
                break

        # Temporary placeholder
        return "placeholder"