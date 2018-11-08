import psycopg2

conn = psycopg2.connect('dbname=project2 user=postgres password=root')
cur = conn.cursor()
cur.execute("EXPLAIN (FORMAT JSON) SELECT * FROM author LIMIT 10;")
print(cur.fetchall())