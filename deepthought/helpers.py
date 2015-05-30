from boto.s3.connection import S3Connection
from boto.s3.key import Key
from config import boto_access, boto_secret
import logging
import zipfile
import bz2
import os

module_logger = logging.getLogger(__name__)


class S3Bucket(object):
    """ Helper functions related to S3 file operations

    Attributes:
        bucket          S3 Bucket Object
        conn            S3 Connection Object
    """
    conn = None
    bucket = None

    def __init__(self, bucket_name='twitter-deepthought'):
        # Intialize logger
        self.logger = logging.getLogger(__name__)

        # Authenticate with Amazon S3
        self.logger.debug("Authenticating with Amazon S3")
        self.conn = S3Connection(boto_access, boto_secret)

        # Check that bucket actually exists
        self.logger.debug("Checking if bucket '" + bucket_name + "' exists")
        exists = self.conn.lookup(bucket_name)
        if exists is None:
            raise ValueError("No such bucket")

        # If bucket exist, get it
        self.logger.debug("Accessing bucket '" + bucket_name + "'")
        self.bucket = self.conn.get_bucket(bucket_name)

    def list_keys(self):
        """
        List the keys in this bucket
        :return: List of keys
        """
        self.logger.debug("Listing keys in bucket '" + self.bucket.name + "'")
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
        return list(reversed(key_list[-num:]))

    def find_key(self, key_name):
        """
        Given the name of a key, iterate through all keys in this bucket
        and return specified key. Else, return none
        :param key_name: Name of the key to be searched for
        :return:
        """
        self.logger.debug("Finding key with name of '" + key_name + "'")
        for key in self.list_keys():
            if key_name in key.name:
                return key
        return None

    def upload(self, file_path, key=""):
        """
        Upload a file to this bucket
        :param file_path: Path of file to be uploaded
        :param key: Unique key of the file, defaults to the file_path
        """
        if key == "":
            key = file_path

        # Make a key for this file
        k = Key(self.bucket)
        k.key = key

        self.logger.info("Uploading " + file_path)

        # Try to upload the file
        try:
            k.set_contents_from_filename(file_path)
        except:
            raise

    @staticmethod
    def download(key):
        """
        Given a key, download the corresponding file
        :param key: Unique key
        :return File path to downloaded file
        """
        module_logger.info("Downloading " + key.name)
        key.get_contents_to_filename(key.name)
        return key.name


def unpack(file_path):
    """
    Unzip and decompress a dir and its files
    :param file_path: Path to the zipped dir
    """
    module_logger.info("Unpacking " + file_path)
    # Load the zip file
    ziph = zipfile.ZipFile(file_path, "r")

    # Extract all files in the Zipfile
    ziph.extractall()

    # Close the zipfile handle
    ziph.close()

    # Delete the zip file
    os.remove(file_path)

    # Get dir where unzipped files are stored
    dir = os.path.splitext(file_path)[0]

    for root, dirs, files in os.walk(dir):
        for name in files:
            # Get the file path of current file
            file_path = os.path.join(root, name)

            # Open the original file and the compressed file
            original_f = open(os.path.splitext(file_path)[0], 'wb')
            compressed_f = bz2.BZ2File(file_path, 'rb')

            # Read the compressed file chunk by chunk (chunk size of 1MB)
            # and write the uncompressed contents into a new file
            for data in iter(lambda: compressed_f.read(1024 * 1024), b''):
                original_f.write(data)

            # Close both files
            original_f.close()
            compressed_f.close()

            # Remove the old, compressed file
            os.remove(file_path)


def upload_dir(dir_path, key=None):
    """
    Given a dir, add its files to a Zipfile and upload the Zipfile
    :param dir_path: The path of the dir to be uploaded
    :param key: The key to be used when uploading the dir
    """
    # File path of zipped dir
    file_path = dir_path + '.zip'
    # Set key to be same as file path is none is set
    if key is None:
        key = file_path
    # Zip the dir
    zip_f = zipfile.ZipFile(file_path, 'w')
    for root, dirs, files in os.walk(dir_path):
        for f in files:
            zip_f.write(os.path.join(root, f))
    zip_f.close()
    # Try to upload the zipped dir
    bucket = S3Bucket()
    try:
        bucket.upload(file_path, key)
    except:
        module_logger.error("Upload of " + file_path + " failed!")
    # Delete file after upload
    os.remove(file_path)


def read_file_in_chunks(file_object, chunk_size=1024 * 1024):
    """ Generator to read a file piece by piece.
    Default chunk size: 1MB """
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data