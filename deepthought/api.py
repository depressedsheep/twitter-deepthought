"""
This module provides a way for the frontend to communicate with the program,
through this RESTful web API
"""
import logging
import threading

import flask
import flask_restful
from flask_restful import Resource

import app
import search
import config


class API(threading.Thread):

    """Runs a Flask server to serve the API"""

    CrawlerStatus_url = 'crawler/status'
    Search_url = 'search/<string:query>'

    def __init__(self):
        """Initializes the API thread"""
        super(API, self).__init__()
        self.logger = logging.getLogger(__name__)

    def run(self):
        """Setups the API server"""
        # Initializes the server
        app = flask.Flask(__name__)
        api = flask_restful.Api(app)

        # Setup the routes for the server
        api_base_url = config.api_base_url
        api.add_resource(CrawlerStatus, api_base_url + self.CrawlerStatus_url)
        api.add_resource(Search, api_base_url + self.Search_url)

        # Run the Flask server on the specified port
        # The server is not run on the default port to prevent clashes
        self.logger.warn("API webservice started")
        app.run(debug=False, port=config.api_port)

    def stop(self):
        """Stop the API server"""
        self.logger.warn("API webservice stopped")


class CrawlerStatus(Resource):
    """Handle requests for the crawler's status"""

    @staticmethod
    def get():
        """Handle GET requests

        Returns:
            status (dict): The status of the crawler
        """
        return app.threads['crawler'].status


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





