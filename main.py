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


def main():
    # Init logging
    init_logging()

    # Create a shared queue of files to be analysed
    file_queue = Queue.Queue()

    # Run crawler
    c = deepthought.crawler.Crawler()
    ct = threading.Thread(target=c.start, args=(file_queue,))
    ct.start()
    ct.name = "Crawler thread"

    # Run spike detector
    a = deepthought.analyser.Analyser()
    at = threading.Thread(target=a.start, args=(file_queue,))
    at.start()
    at.name = "Analyser thread"

    # Run RESTful API web service
    r = deepthought.api.API()
    rt = threading.Thread(target=r.run)
    rt.start()
    rt.name = "API webservice thread"


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
    console_handler.setLevel(logging.INFO)

    # Add the handlers to the logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


if __name__ == '__main__':
    main()