from boto.s3.connection import S3Connection
from config import boto_access, boto_secret
import logging
import zipfile
import zlib
import os


class S3Bucket(object):
    """ Accepts Twitter tweet stream and save them hourly

    Attributes:
        bucket          S3 Bucket Object
        conn            S3 Connection Object
    """
    conn = None
    bucket = None

    def __init__(self, bucket_name='twitter-deepthought'):
        # Authenticate with Amazon S3
        logging.info("Authenticating with Amazon S3")
        self.conn = S3Connection(boto_access, boto_secret)

        # Check that bucket actually exists
        logging.info("Checking if bucket '" + bucket_name + "' exists")
        exists = self.conn.lookup(bucket_name)
        if exists is None:
            raise ValueError("No such bucket")

        # If bucket exist, get it
        logging.info("Accessing bucket '" + bucket_name + "'")
        self.bucket = self.conn.get_bucket(bucket_name)

    def list_keys(self):
        """
        List the keys in this bucket
        :return: List of keys
        """
        return list(self.bucket.list())

    def list_recent_keys(self, num):
        """
        List recently added keys in the bucket
        :param num: Number of recent keys to list
        :return: List of recent keys
        """
        # Get list of all the keys in this bucket
        key_list = self.list_keys()

        # Sort the list of keys by last modified
        key_list.sort(key=lambda x: x.last_modified)

        # Return the last X number of keys
        return list(reversed(key_list[-num-1:-1]))

    @staticmethod
    def download(key):
        """
        Given a key, download the corresponding file
        :param key: Unique key
        :return Filepath to downloaded file
        """
        logging.info("Downloading " + key.name)
        key.get_contents_to_filename(key.name)


def decompress_dir(file_path):
    """
    Unzip and decompress a dir and its files
    :param file_path: Path to the zipped dir
    """
    # Load the zip file
    ziph = zipfile.ZipFile(file_path, "r")

    # Make new dir to store decompressed files
    dir_name = os.path.splitext(file_path)[0]
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    for file_name in ziph.namelist():
        # Read the compressed contents into a string
        compressed_contents = ziph.read(file_name)
        # Decompress the string
        decompressed_contents = zlib.decompress(compressed_contents, 16 + zlib.MAX_WBITS)
        # Remove the '.gz' extension for the new file path
        new_file_path = os.path.splitext(file_name)[0]
        # Write the decompressed contents to the new file
        with open(new_file_path, "w") as f:
            f.write(decompressed_contents)

    # Close the zipfile handle
    ziph.close()

    # Remove the old zipped dir
    os.remove(file_path)