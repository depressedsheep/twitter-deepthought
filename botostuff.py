from boto.s3.connection import S3Connection
from boto.s3.key import Key as botoKey
try:
	from config import boto_access, boto_secret
except:
	pass

def save(key, filename, BUCKET_NAME = 'twitter-deepthought'):
	conn = S3Connection(boto_access, boto_secret)
	try:
		bucket = conn.create_bucket(BUCKET_NAME, location =Location.SAEast)
	except:
		bucket = conn.get_bucket(BUCKET_NAME)
	k = Key(bucket)
	k.key = key
	k.set_contents_from_filename(filename)
def load(key, BUCKET_NAME = 'twitter-deepthought'):
	conn = S3Connection(boto_access, boto_secret)
	bucket = conn.get_bucket(BUCKET_NAME)
	k = Key(bucket)
	k.key = key
	print bucket.list()
if __name__ == '__main__':
	print "ASDF"