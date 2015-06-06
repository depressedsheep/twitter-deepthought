import logging

import spike

# import deepthought
import helpers


class Analyser(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spike_detector = spike.SpikeDetector()
        # self.deepthought = deepthought.deepthought()

    def start(self, file_queue):
        """
        Main function to start analyses of files collected
        :param file_queue: Shared queue between Crawler and Spike threads of files to be analysed
        """
        self.logger.warn("Analyser started")
        
        while True:
            # Wait until a file path is received
            file_path = file_queue.get(block=True)
            self.spike_detector.find_spikes(file_path)

            # Upload and delete the dir after analysis
            helpers.upload_dir(file_path)
