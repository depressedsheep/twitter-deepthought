"""This module communicates with the crawler and processes incoming files"""
import logging
import threading
import os
import re

from deepthought import config, helpers
import crawler
from deepthought.processing import analyser


class Processor(threading.Thread):
    """Processes incoming files from the crawler"""

    def __init__(self):
        """Initializes the Processor

        Args:
            file_queue (Queue.Queue): The shared queue between the Crawler and Processor to send files
        """
        super(Processor, self).__init__()
        self.logger = logging.getLogger(__name__)

    def run(self):
        """Main function to start processing of files received from the crawler

        This is done in a infinite while loop. In each iteration, the Processor will wait for the Crawler to put a file
        into the shared Queue.

        When it receives a file, it will start analysis.

        After analysis, the directory, along with the results of the analysis, will be uploaded to Amazon S3 servers.
        """
        self.logger.warn("Processor started")
        self.scan_dir()

    def scan_dir(self):
        def analyse_dir(dir):
            a = analyser.Analyser()
            try:
                a.analyse(dir)
            except ValueError:
                self.logger.error(dir + " is not a valid file path!?")

            if not config.DEV_MODE:
                helpers.upload_dir(dir)

        threads = list()
        dirs = next(os.walk(config.working_dir))[1]
        pattern = re.compile("\d{2}-\d{2}-\d{4}_\d{2}")
        for dir in dirs:
            if os.path.join(config.working_dir, dir) == crawler.Crawler.get_curr_hour() or not pattern.match(dir) or len(dir) != 13:
                dirs.remove(dir)
                continue

            t = threading.Thread(target=analyse_dir, args=(os.path.join(config.working_dir, dir),))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        threading.Timer(5 * 60, self.scan_dir).start()

    def stop(self):
        """Stops the Processor

        It also checks if there were any queued files when the Processor was stopped.
        """
        self.logger.warn("Processor stopped")