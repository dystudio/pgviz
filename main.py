import psycopg2
from db_credentials import DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD
from db_connection import get_execution_plan, connect_to_db

conn = connect_to_db(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
cur = conn.cursor()
final_query = get_execution_plan("SELECT * FROM author LIMIT 10;")
cur.execute(final_query)
print(cur.fetchall())