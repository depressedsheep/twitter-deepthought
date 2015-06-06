import logging
import threading
import os
import Queue
from time import strftime

import deepthought.config
import deepthought.crawler
import deepthought.analyser
import deepthought.helpers
import deepthought.api


class App(object):
    def __init__(self):
        # Init logging
        self.init_logging()

    @staticmethod
    def run():
        # Create a shared queue of files to be analysed
        file_queue = Queue.Queue()

        # Run crawler
        crawler = deepthought.crawler.Crawler()
        crawler_thread = threading.Thread(target=crawler.start, args=(file_queue,))
        crawler_thread.start()
        crawler_thread.name = "Crawler thread"

        # Run analyser
        analyser = deepthought.analyser.Analyser()
        analyser_thread = threading.Thread(target=analyser.start, args=(file_queue,))
        analyser_thread.start()
        analyser_thread.name = "Analyser thread"

        # Run API web service
        api = deepthought.api.API()
        api_thread = threading.Thread(target=api.run)
        api_thread.start()
        api_thread.name = "API webservice thread"

    @staticmethod
    def init_logging():
        """
        Initialize logging to file and console
        """
        # Create format for logging
        log_formatter = logging.Formatter(
            fmt="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
            datefmt="%m/%d/%Y %I:%M %p")
        # Get root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

        # Check if log dir exists, else create it
        log_dir = deepthought.config.log_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Create path where log file is going to be stored
        log_file = strftime("%d-%m-%Y_%H-%M-%S") + '.log'
        log_path = os.path.join(log_dir, log_file)

        # Create file handler which logs all messages with level DEBUG and above
        file_handler = logging.FileHandler(log_path, 'w')
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(logging.DEBUG)

        # Create console handler which logs all messages with level INFO and above
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(logging.WARN)

        # Add the handlers to the logger
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)