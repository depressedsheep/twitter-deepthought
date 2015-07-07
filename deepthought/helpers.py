"""This module provides helper functions for other modules"""

import logging
import bz2
import os
import shutil
import threading

from boto.s3.connection import S3Connection, Location
from boto.s3.key import Key
from boto.exception import BotoClientError, BotoServerError

import config


module_logger = logging.getLogger(__name__)


class S3Bucket(object):
    """ Helper functions related to S3 file operations

    Attributes:
        bucket: S3 Bucket Object
        conn: S3 Connection Object
    """

    def __init__(self, bucket_name=None):
        """Initializes the bucket"""
        # Initialize logger
        self.logger = logging.getLogger(__name__)

        # Check if a bucket name was specified
        # If not, set it to the default bucket name specified in the config
        if bucket_name is None:
            bucket_name = config.bucket_name

        # Authenticate with Amazon S3
        self.logger.debug("Authenticating with Amazon S3")
        self.conn = S3Connection(config.boto_access, config.boto_secret)

        # Check if bucket exists
        self.logger.debug("Checking if bucket '" + bucket_name + "' exists")
        exists = self.conn.lookup(bucket_name)

        # If the bucket doesn't exist, create a new bucket
        if exists is None:
            self.logger.warn("Bucket '" + bucket_name + "' doesn't exist, creating")
            try:
                # Note: Location.APSoutheast refers to Singapore
                self.conn.create_bucket(bucket_name, location=Location.APSoutheast)
            except:
                self.logger.error("Error creating bucket!")
                raise ValueError

        # Access the bucket
        self.logger.debug("Accessing bucket '" + bucket_name + "'")
        self.bucket = self.conn.get_bucket(bucket_name)

    def list_keys(self):
        """List the keys in the bucket, sorted by the last modified date

        Returns
            key_list (list): The list of keys
        """
        self.logger.debug("Listing keys in bucket '" + self.bucket.name + "'")

        key_list = list(self.bucket.list())
        key_list.sort(key=lambda x: x.last_modified)
        return key_list

    def find_key(self, key_name):
        """Find a key in the bucket by name

        Args:
            key_name (str): The name of the key to be searched for

        Returns:
            key: The key if found, else returns None
        """
        self.logger.debug("Finding key with name of '" + key_name + "'")
        for key in self.list_keys():
            if key_name in key.name:
                return key
        return None

    def find_keys(self, key_name):
        self.logger.debug("Finding all keys with name of '" + key_name + "'")
        key_list = list()
        for key in self.list_keys():
            if key_name in key.name:
                key_list.append(key)
        return key_list


    def upload(self, file_path, key_name=None):
        """Upload a file to this bucket

        Args:
            file_path (str): Path of the file to be uploaded
            key_name (str): The name of the key to upload the file as, defaults to the file path
        """
        if key_name is None:
            key_name = file_path

        # Make a key using the specified key name
        k = Key(self.bucket)
        k.key = key_name

        self.logger.info("Uploading " + file_path)
        try:
            k.set_contents_from_filename(file_path)
        except (BotoClientError, BotoServerError):
            self.logger.error("Upload of '" + file_path + "' failed!")
            raise

    @staticmethod
    def download(key, file_path=None):
        """Given a key, download the file

        Args:
            key: The key of the file to be downloaded
            file_path (str): The file path where the file should be downloaded to, defaults to the name of the key

        Returns:
            file_path (str): The file path where the downloaded file is stored

        """
        module_logger.info("Downloading " + key.name)
        if file_path is None:
            file_path = key.name.replace("/", "\\")

        d = os.path.dirname(file_path)
        if not os.path.exists(d):
            os.makedirs(d)

        key.get_contents_to_filename(file_path)
        return file_path

    @staticmethod
    def download_async(key_list, dir_path=""):
        def download_thread(k):
            b = S3Bucket()
            b.download(k, os.path.join(dir_path, k.name.replace("/", "\\")))

        for key in key_list:
            t = threading.Thread(target=download_thread, args=(key,))
            t.start()
            t.join()


def upload_dir(dir_path):
    """Upload a directory to Amazon S3

    This function iterates over the files in the directory, compresses them, then uploads them individually to the server.

    Then, the directory is deleted to save space.

    Args:
        dir_path (str): The path to the directory to be uploaded
    """
    module_logger.debug("Processing dir '" + dir_path + "'")

    bucket = S3Bucket()
    for root, dirs, files in os.walk(dir_path):
        for name in files:
            # Get the file path of current file
            file_path = os.path.join(root, name)

            # Compress the file
            compress_file(file_path)
            file_path = os.path.join(dir_path, name) + ".bz2"

            # Upload the file
            try:
                bucket.upload(file_path, file_path.replace("\\", "/"))
            except (BotoServerError, BotoClientError):
                module_logger.error("Upload of " + file_path + " failed!")
                raise

            # Remove the old, uncompressed file
            os.remove(file_path)

    # Delete the directory
    shutil.rmtree(dir_path)


def compress_file(file_path):
    """Compress a file using BZ2

    Args:
        file_path (str): The path to the file to be compressed

    Returns:
        file_path (str): Returns the file path to the compressed file
    """
    logging.debug("Compressing '" + file_path + "'")

    # Open the original file for reading the compressed file for writing
    original_f = open(file_path)
    compressed_fp = file_path + '.bz2'
    compressed_f = bz2.BZ2File(compressed_fp, 'w')

    # Read the original file chunk by chunk and write it to the compressed file
    for chunk in read_file_in_chunks(original_f):
        compressed_f.write(chunk)

    original_f.close()
    compressed_f.close()
    os.remove(file_path)
    return compressed_fp


def decompress_file(file_path):
    """Decompress a BZ2 compressed file

    Args:
        file_path (str): The path to the file to be decompressed

    Returns:
        file_path (str): Returns the file path to the decompressed file
    """
    logging.debug("Decompressing '" + file_path + "'")

    # Open the original file for writing the compressed file for reading
    original_fp = os.path.splitext(file_path)[0]
    original_f = open(original_fp, 'wb')
    compressed_f = bz2.BZ2File(file_path, 'rb')

    # Read the compressed file chunk by chunk and write it to the decompressed file
    for data in iter(lambda: compressed_f.read(1024 * 1024), b''):
        original_f.write(data)

    original_f.close()
    compressed_f.close()
    os.remove(file_path)
    return original_fp


def read_file_in_chunks(file_object, chunk_size=10 * 1024 * 1024):
    """Reads a file in chunks due to large file sizes and memory limitations

    Args:
        file_object (file): The file to be read
        chunk_size (int): The size of the chunks in Bytes. Defaults to 10MB
    """
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data