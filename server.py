from os import sys, path

# Set searching dependency packages, no need to set pythonpath
root_dir = path.dirname(path.abspath(__file__))
sys.path.append(root_dir)

from flask import Flask
from flask_restful import Resource, Api, reqparse
import psycopg2

from db_credentials import DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD
from db_connection import get_explain_query, connect_to_db
from utils.traverse import traverse_json

# Initialize flask application
app = Flask(__name__)
api = Api(app)

# Connect to database
conn = connect_to_db(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
cur = conn.cursor()

# Define flask endpoint
class QueryExplainer(Resource):
    def get(self, query):
        final_query = get_explain_query(query)
        cur.execute(final_query)
        
        explain_json = cur.fetchone()
        result = traverse_json(explain_json[0][0]["Plan"], query)

        return result

api.add_resource(QueryExplainer, '/explain/<string:query>')

# Run application script
if __name__ == '__main__':
    app.run(debug=True)