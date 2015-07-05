"""This module communicates with the crawler and processes incoming files"""
import logging
import threading

import analyser
import helpers


class Processor(threading.Thread):
    """Processes incoming files from the crawler

    Attributes:
        file_queue (Queue.Queue): The shared queue between the Crawler and Processor to send files
        stopped (bool): Boolean value indicating if Processor has stopped
    """

    def __init__(self, file_queue):
        """Initializes the Processor

        Args:
            file_queue (Queue.Queue): The shared queue between the Crawler and Processor to send files
        """
        super(Processor, self).__init__()
        self.logger = logging.getLogger(__name__)

        self.file_queue = file_queue
        self.stopped = False

    def run(self):
        """Main function to start processing of files received from the crawler

        This is done in a infinite while loop. In each iteration, the Processor will wait for the Crawler to put a file
        into the shared Queue.

        When it receives a file, it will start analysis.

        After analysis, the directory, along with the results of the analysis, will be uploaded to Amazon S3 servers.
        """
        self.logger.warn("Processor started")

        # Analyser object to be used to analyse files received
        a = analyser.Analyser()
        while True:
            # Break out of the loop if Processor has been stopped
            if self.stopped:
                break

            # Wait for the Crawler to put something into the shared queue
            file_path = self.file_queue.get(block=True)
            self.logger.warn("File '" + file_path + "' received by processor")

            # Start analysis of the files
            a.analyse(file_path)

            # Upload and delete the dir after analysis
            helpers.upload_dir(file_path)

    def stop(self):
        """Stops the Processor

        It also checks if there were any queued files when the Processor was stopped.
        """
        self.stopped = True

        # Informs the user if there were queued files when the Processor was stopped
        num_queued_files = str(self.file_queue.qsize())
        if num_queued_files > 0:
            self.logger.warn("Processor stopped with " + num_queued_files + " queued files")
