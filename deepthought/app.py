"""This module initializes core functions such as logging and starts the threads. It is usually invoked by main.py

Attributes:
    threads (list): A list of running threads
"""

import logging
import Queue

from deepthought import helpers, console
from deepthought.processing import processor
from deepthought.api import api_server
# List of threads
threads = {}


class App(object):
    """Initialize app and start threads"""

    def __init__(self):
        """Initializes logging and thread_names"""
        helpers.init_logging()
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def start():
        """Starts all the threads with required variables"""

        # Init threads to be started
        global threads
        threads['processor'] = processor.Processor()
        threads['api'] = api_server.APIServer()
        console_thread = console.Console()

        # Start the threads
        for thread_name, thread in threads.iteritems():
            thread.start()

        # Starts console thread
        console_thread.run()