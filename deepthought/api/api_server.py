"""
This module provides a way for the frontend to communicate with the program,
through this RESTful web API
"""
import logging
import threading
import collections
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

    CrawlerTps_url = 'tps/<string:date>'
    S3Dates_url = 's3/dates'
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
        api.add_resource(CrawlerTps, api_base_url + self.CrawlerTps_url)
        api.add_resource(Search, api_base_url + self.Search_url)
        api.add_resource(S3Dates, api_base_url + self.S3Dates_url)

        # Run the Flask server on the specified port
        # The server is not run on the default port to prevent clashes
        self.logger.warn("API webservice started")
        self.app.run(debug=False, port=config.api_port)

    def stop(self):
        """Stop the API server"""
        self.logger.warn("API webservice stopped")


class CrawlerTps(Resource):
    """Handle requests for the crawler's status"""

    @staticmethod
    def get(date):
        """Handle GET requests

        Returns:
            status (dict): The status of the crawler
        """
        b = helpers.S3Bucket()
        k = b.find_key(date + "/tps.csv.bz2")
        if k is None or len(date) != 13:
            return {"error": "Invalid date provided"}
        fp = os.path.join(config.working_dir, str(uuid.uuid4()) + ".bz2")
        b.download(k, fp)
        fp = helpers.decompress_file(fp)
        d = {}
        with open(fp, 'r') as f:
            dr = csv.DictReader(f)
            for r in dr:
                d[r['timestamp']] = r['tps']

        os.remove(fp)
        return collections.OrderedDict(sorted(d.items()))


class Search(Resource):
    """Handle search requests"""

    @staticmethod
    def get(query):
        """Handle GET requests

        Args:
            query (str): The keyword to be searched

        Returns:
            result (collections.OrderedDict): The results of the query
        """
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