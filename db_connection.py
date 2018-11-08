import psycopg2

def connect(dbname, user, password):
  connect_argument = 'dbname=' + dbname + ', ' + 'user=' + user + ', ' + 'password=' + password
  conn = psycopg2.connect(connect_argument)