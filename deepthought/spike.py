from __future__ import division
from config import public_dir, spike_threshold, ema_length, growth_length
import json
import collections
import helpers
import os
import logging
import shutil


class SpikeDetector(object):
    """ Find if any spikes occurred every hour

    Attributes:
        fp      The file path to the downloaded dir
    """

    fp = ""

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
            self.fp = file_path
            self.find_spikes()

    def find_spikes(self):
        """
        Given a file name, attempt to find the spikes that occurred within that hour by calculating EMA,
        EMA growth and then checking if said growth exceeds the threshold
        :param file_name: Name of file to be analysed
        """
        self.logger.info("Starting spike detection of file '" + self.fp + "'")

        # Load the TPS data from the file into the dict
        tps_f = open(os.path.join(self.fp, "tps.json"), 'r')
        tps_f_contents = tps_f.read()
        tps_f.close()
        tps_dict = json.loads(tps_f_contents)

        # Convert the timestamps from strings to integers
        tps_dict = {int(float(k)): v for k, v in tps_dict.items()}

        # Initialize variables for storing EMA values, growth values, and timestamps of spikes
        ema = []
        growth = {}
        spikes = {}

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
                    spikes.append({timestamp: self.find_spike_contents(timestamp)})

        # Dict of file paths to be used later
        fps = {
            "ema": os.path.join(self.fp, "ema.json"),
            "growth": os.path.join(self.fp, "growth.json"),
            "tps": os.path.join(self.fp, "tps.json"),
            "spikes": os.path.join(self.fp, "spikes.json")
        }

        # Combine the list of dicts into one big OrderedDict
        ordered_ema = collections.OrderedDict((k, v) for d in ema for (k, v) in d.items())
        with open(fps[ema], 'w') as f:
            self.logger.debug("Writing to " + fps[ema])
            f.write(json.dumps(ordered_ema))

        # Sort the dict of growths and store it in an OrderedDict
        ordered_growth = collections.OrderedDict(sorted(growth.items()))
        with open(fps["growth"], 'w') as f:
            self.logger.debug("Writing to " + fps["growth"])
            f.write(json.dumps(ordered_growth))

        # Sort the dict of tps and store it in an OrderedDict
        ordered_tps = collections.OrderedDict(sorted(tps_dict.items()))
        with open(fps["tps"], 'w') as f:
            self.logger.debug("Writing to " + fps["tps"])
            f.write(json.dumps(ordered_tps))

        # Sort the dict of spikes and store it in an OrderedDict
        ordered_spikes = collections.OrderedDict(sorted(spikes.items()))
        with open(fps["spikes"], 'w') as f:
            self.logger.debug("Writing to " + fps["spikes"])
            f.write(json.dumps(ordered_spikes))

        self.logger.info("Spike detection done and recorded for '" + self.fp + "'")

    def find_spike_contents(self, timestamp):
        """
        Given the timestamp of the spike, find the contents of tweets during the spike
        :param timestamp: Timestamp when spike happened
        :return: Returns the top 5 words used in tweets during the spike
        """
        pass