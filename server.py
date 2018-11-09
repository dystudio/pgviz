from flask import Flask
from flask_restful import Resource, Api, reqparse
import psycopg2
from db_credentials import DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD
from db_connection import get_execution_plan, connect_to_db

# Initialize flask application
app = Flask(__name__)
api = Api(app)

# Connect to database
conn = connect_to_db(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
cur = conn.cursor()

# Define flask endpoint
class QueryExplainer(Resource):
    def get(self, query):
        final_query = get_execution_plan(query)
        cur.execute(final_query)
        return cur.fetchall()

api.add_resource(QueryExplainer, '/explain/<string:query>')

# Run application script
if __name__ == '__main__':
    app.run(debug=True)