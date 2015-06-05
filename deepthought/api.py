import flask
import flask_restful


class API(object):
    def __init__(self):
        self.app = flask.Flask(__name__)
        self.api = flask_restful.Api(self.app)

    def run(self):
        self.api.add_resource(CrawlerStatus)
        self.app.run(debug=True)


class CrawlerStatus(flask_restful.Resource):
    def get(self):
        return {'hello': 'world'}
