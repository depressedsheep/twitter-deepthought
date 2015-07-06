"""This module initializes core functions such as logging and starts the threads. It is usually invoked by main.py

Attributes:
    threads (list): A list of running threads
"""

import logging
from time import strftime
import os
import Queue

from deepthought import config, helpers, console
from deepthought.crawler import crawler
from deepthought.processing import processor
from deepthought.api import api_server


# List of threads
threads = {}


class App(object):
    """Initialize app and start threads"""

    def __init__(self):
        """Initializes logging and thread_names"""
        self.init_logging()
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def start():
        """Starts all the threads with required variables"""
        # Create a shared queue of files to be analysed
        file_queue = Queue.Queue()

        # Init threads to be started
        global threads
        threads['processor'] = processor.Processor(file_queue)
        threads['crawler'] = crawler.Crawler(file_queue)
        threads['api'] = api_server.APIServer()
        console_thread = console.Console()

        # Start the threads
        for thread_name, thread in threads.iteritems():
            thread.start()

        # Starts console thread
        console_thread.run()

    @staticmethod
    def init_logging():
        """Initialize logging to file and console.

        Log file has the format "app.log.%d-%m-%Y_%H-%M-%S"

        Messages with level DEBUG and above gets logged to the file.

        Messages with level INFO and above gets logged to the console.
        """

        # Create format for logging
        log_formatter = logging.Formatter(
            fmt="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
            datefmt="%m/%d/%Y %I:%M %p")

        # Get root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

        # Check if log dir exists, else create it
        log_dir = os.path.join(config.log_dir, "")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Create path where log file is going to be stored
        log_file = 'app.log'
        log_path = os.path.join(log_dir, log_file)

        # Create file handler which logs all messages with level DEBUG and above
        # This file handler will change file every hour
        file_handler = logging.handlers.TimedRotatingFileHandler(log_path)
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(logging.DEBUG)

        # Create console handler which logs all messages with level INFO and above
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(logging.INFO)

        # Add the handlers to the logger
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)