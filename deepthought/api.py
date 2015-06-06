import os
import json
import logging

import flask
import flask_restful
from flask_restful import Resource

import config


class API(object):
    def __init__(self):
        self.app = flask.Flask(__name__)
        self.api = flask_restful.Api(self.app)
        self.logger = logging.getLogger(__name__)

    def run(self):
        self.api.add_resource(CrawlerStatus, '/api/crawler/status')
        self.logger.warn("API webservice started")
        self.app.run(debug=False, port=config.api_port)


class CrawlerStatus(Resource):
    def get(self):
        status_file = open(os.path.join(config.working_dir, "status.json"))
        return json.load(status_file)
