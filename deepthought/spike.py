from __future__ import division
import threading
from config import public_dir, spike_threshold, ema_length, growth_length
import json
import collections
import helpers
import os
import logging
import shutil


class SpikeDetector(object):
    """ Find if any spikes occurred every hour
    """

    def __init__(self):
        # Intialize logger
        self.logger = logging.getLogger(__name__)

    def find_spikes(self):
        """
        At every hour, attempt to find the spikes that occurred within that hour by calculating EMA,
        EMA growth and then checking if said growth exceeds the threshold
        """
        self.logger.info("Starting spike detection")
        # Call this function again one hour from now
        t = threading.Timer(60 * 60, self.find_spikes)
        t.start()

        # Download and unpack the latest file added to the bucket
        bucket = helpers.S3Bucket()
        key = bucket.list_recent_keys(1)[0]
        fp = bucket.download(key)
        helpers.unpack(fp)
        # Remove the .zip extension from the dir path
        fp = os.path.splitext(fp)[0]
        # Load the TPS data from the current hour into the dict
        tps_f = open(os.path.join(fp, "tps.json"), 'r')
        tps_f_contents = tps_f.read()
        tps_f.close()
        tps_dict = json.loads(tps_f_contents)
        # Convert the timestamps from strings to integers
        tps_dict = {int(float(k)): v for k, v in tps_dict.items()}
        # Done with files, delete directory to save space
        shutil.rmtree(fp)

        # Initialize variables for storing EMA values, growth values, and timestamps of spikes
        ema = []
        growth = {}
        spikes = []

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
                    spikes.append(timestamp)

        # Combine the list of dicts into one big OrderedDict
        ordered_ema = collections.OrderedDict((k, v) for d in ema for (k, v) in d.items())
        # File path where the graphing data is to be stored, for debugging purposes
        ema_graph_fp = public_dir + 'ema.json'
        # Write to said file
        with open(ema_graph_fp, 'w') as f:
            self.logger.debug("Writing to " + ema_graph_fp)
            f.write(json.dumps(ordered_ema))

        # Sort the dict of growths and store it in an OrderedDict
        ordered_growth = collections.OrderedDict(sorted(growth.items()))
        # Same operation as above
        growth_graph_fp = public_dir + 'growth.json'
        with open(growth_graph_fp, 'w') as f:
            self.logger.debug("Writing to " + growth_graph_fp)
            f.write(json.dumps(ordered_growth))

        # Sort the dict of tps and store it in an OrderedDict
        ordered_tps = collections.OrderedDict(sorted(tps_dict.items()))
        tps_graph_fp = public_dir + "tps.json"
        with open(tps_graph_fp, 'w') as f:
            self.logger.debug("Writing to " + tps_graph_fp)
            f.write(json.dumps(ordered_tps))

        spikes_fp = public_dir + "spikes.json"
        with open(spikes_fp, 'a') as f:
            self.logger.debug("Writing to " + spikes_fp)
            for spike in spikes:
                f.write(str(spike) + '\n')

        self.logger.info("Spike detection done for this hour")