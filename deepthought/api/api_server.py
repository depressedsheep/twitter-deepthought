"""
This module provides a way for the frontend to communicate with the program,
through this RESTful web API
"""
import logging
import threading
import collections
import shutil
import os
import csv
import uuid

import flask
import flask_restful

from flask_restful import Resource

from deepthought import config, helpers
from deepthought.api import search


class APIServer(threading.Thread):
    """Runs a Flask server to serve the API"""

    S3Dates_url = 's3/dates'
    S3Stats_url = "s3/stats/<string:date>"
    Search_url = 'search/<string:query>'

    def __init__(self):
        """Initializes the API thread"""
        super(APIServer, self).__init__()
        self.logger = logging.getLogger(__name__)
        self.app = None

    def run(self):
        """Setups the API server"""
        # Initializes the server
        self.app = flask.Flask(__name__)
        api = flask_restful.Api(self.app)

        # Setup server logging such that only errors are displayed
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)

        # Setup the routes for the server
        api_base_url = config.api_base_url
        api.add_resource(S3Stats, api_base_url + self.S3Stats_url)
        api.add_resource(Search, api_base_url + self.Search_url)
        api.add_resource(S3Dates, api_base_url + self.S3Dates_url)

        # Run the Flask server on the specified port
        # The server is not run on the default port to prevent clashes
        self.logger.warn("API webservice started")
        self.app.run(debug=False, port=config.api_port, host='0.0.0.0')

    def stop(self):
        """Stop the API server"""
        self.logger.warn("API webservice stopped")


class S3Stats(Resource):
    @staticmethod
    def get(date):
        b = helpers.S3Bucket()
        kl = b.find_keys(date)
        if len(kl) == 0 or len(date) != 13:
            return {"error": "Invalid date provided"}

        dp = os.path.join(config.working_dir, str(uuid.uuid4()))
        for k in kl:
            if any(ss in k.name for ss in ['tps', 'ema', 'growth', 'spikes']):
                b.download(k, os.path.join(dp, k.name))

        stats = collections.defaultdict(dict)

        def proc_file(fp):
            s = fp.split(os.sep)[-1].split(".")[0].lower()
            if fp.lower().endswith(".bz2"):
                fp = helpers.decompress_file(fp)
            with open(fp, 'r') as f:
                dr = csv.DictReader(f)
                for r in dr:
                    stats[s][r['timestamp']] = r[s]

        threads = []
        for subdir, dirs, files in os.walk(dp):
            for file in files:
                file_path = os.path.join(subdir, file)
                t = threading.Thread(target=proc_file, args=(file_path,))
                t.start()
                threads.append(t)

        for t in threads:
            t.join()

        shutil.rmtree(dp)

        s = {}
        for i, d in stats.iteritems():
            s[i] = collections.OrderedDict(sorted(d.items()))
        return s


class Search(Resource):
    @staticmethod
    def get(query):
        return search.search(query)


class S3Dates(Resource):
    @staticmethod
    def get():
        b = helpers.S3Bucket()
        kl = b.find_keys("tps.csv")
        dl = []
        for k in kl:
            dl.append(k.name.split("/")[-2])
        return dl