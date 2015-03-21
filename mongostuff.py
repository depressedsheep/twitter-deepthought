#
# In case importing other modules is too heavy, this might help
# to reduce overhead, and might also make things simpler. 

import pymongo

def save(data, mongo_db, mongo_db_coll, **mongo_conn_kw):
	#default is localhost:27017	
	client = pymongo.MongoClient('mongodb://localhost:27017/')
	db = client[mongo_db]
	coll = db[mongo_db_coll]
	return coll.insert(data)
def load(mongo_db, mongo_db_coll, return_cursor=False, criteria = None, projection=None, **mongo_conn_kw):
	#criteria & projection limits data
	#consider the aggregations framework for more sophiscated queries
	client = pymongo.MongoClient(**mongo_conn_kw)
	db = client[mongo_db]
	coll = db[mongo_db_coll]
	if criteria is None:
		criteria = {}
	if projection is None:
		cursor = coll.find(criteria)
	else:
		cursor = coll.find(criteria, projection)
	if return_cursor:
		return cursor
	else:
		return [item for item in cursor]