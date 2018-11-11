import psycopg2

def connect_to_db(dbname, user, password):
  connect_argument = 'dbname=' + dbname + ' ' + 'user=' + user + ' ' + 'password=' + password
  conn = psycopg2.connect(connect_argument)
  return conn

def get_explain_query(query):
  final_query = 'EXPLAIN (FORMAT JSON) ' + query + ';'
  return final_query