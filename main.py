import deepthought.config
import deepthought.crawler
import deepthought.spike
import logging
import threading
import os
from time import strftime


def main():
    # Set up logging to file and console

    # Create format for logging
    log_formatter = logging.Formatter(
        fmt="[%(levelname)s] [%(asctime)s] [%(module)s.%(funcName)s]: %(message)s",
        datefmt="%m/%d/%Y %I:%M %p")
    # Create logger with 'deepthought'
    logger = logging.getLogger('deepthought')
    logger.setLevel(logging.DEBUG)

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
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Run spike detector
    s = deepthought.spike.SpikeDetector()
    st = threading.Thread(target=s.find_spikes)
    st.start()

    # Run crawler
    c = deepthought.crawler.Crawler()
    ct = threading.Thread(target=c.start)
    ct.start()


if __name__ == '__main__':
    main()