from flask import Flask
from flask_restful import Resource, Api, reqparse
import psycopg2
from qep_traverser import connect_query

# Initialize flask application
app = Flask(__name__)
api = Api(app)


# Define flask endpoint
class QueryExplainer(Resource):
    def get(self, query):
        return connect_query(query)
        
api.add_resource(QueryExplainer, '/explain/<string:query>')

# Run application script
if __name__ == '__main__':
    app.run(debug=True)