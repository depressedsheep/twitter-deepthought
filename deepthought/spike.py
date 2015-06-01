from __future__ import division
import config
import json
import collections
import helpers
import csv
import os
import logging


class SpikeDetector(object):
    """ Given a dir, find if any spikes occurred """

    def __init__(self):
        # Initialize logger
        self.logger = logging.getLogger(__name__)

    def start(self, file_queue):
        """
        Main function to start analyses of files collected
        :param queue: Shared queue between Crawler and Spike threads of files to be analysed
        """
        while True:
            # Wait until a file path is received
            file_path = file_queue.get(block=True)
            self.find_spikes(file_path)

    def find_spikes(self, file_path):
        """
        Given a file path, attempt to find the spikes (if any) that occurred within that hour by calculating EMA,
        EMA growth and then checking if said growth exceeds the threshold
        :param file_path: Path to the dir where data is stored
        """
        self.logger.info("Starting spike detection of dir '" + file_path + "'")

        # Load the TPS data from the file into the dict
        tps_f = open(os.path.join(file_path, "tps.csv"), 'r')
        tps_reader = csv.reader(tps_f)
        tps_dict = dict(filter(None, tps_reader))

        # Convert the timestamps from strings to integers
        tps_dict = {int(float(k)): v for k, v in tps_dict.items()}

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
                    spikes[timestamp] = self.find_spike_contents(timestamp, file_path)

        data = {
            # Combine the list of dicts into one big OrderedDict
            "ema": collections.OrderedDict((k, v) for d in ema for (k, v) in d.items()),

            # Sort the dict of values and store it in an OrderedDict
            "growth": collections.OrderedDict(sorted(growth.items())),
            "tps": collections.OrderedDict(sorted(tps_dict.items())),
            "spikes": collections.OrderedDict(sorted(spikes.items()))
        }

        for (stat, values) in data:
            with open(os.path.join(self.fp, stat + ".csv")) as csvfile:
                field_names = ['timestamp', stat]
                writer = csv.DictWriter(csvfile, fieldnames=field_names)

                writer.writeheader()
                writer.writerows(values)

        self.logger.info("Spike detection done and recorded for '" + self.fp + "'")

    @staticmethod
    def find_spike_contents(timestamp, file_path):
        """
        Given the timestamp of the spike, find the contents of tweets during the spike
        :param timestamp: Timestamp when spike happened
        :param file_path: Path to dir where data is stored
        :return: Returns the top 5 words used in tweets during the spike
        """
        tweets_f = open(os.path.join(file_path, "tweets.csv"))
        tweets_reader = csv.DictReader(tweets_f)
        spike_tweets_text = []
        for tweet in tweets_reader:
            # 1433131173
            tweet_timestamp = int(float(tweet['timestamp']))
            tweet_text = json.loads(tweet['tweet'])['text']
            if timestamp <= tweet_timestamp <= timestamp + config.spike_contents_sample_size:
                spike_tweets_text.append(tweet_text)
            elif tweet_timestamp > timestamp:
                print tweet_timestamp, timestamp, "breaking"
                break

        return "placeholder"