import psycopg2
from config import LOCAL_DB_CREDENTIALS

conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
cur = conn.cursor()
# cur.execute('''CREATE TABLE r1m AS SELECT * FROM submissions LIMIT 1000000''')
cur.execute('''
SELECT table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');''')
r = cur.fetchall()
print(r)
# cur.execute(f'''DROP FOREIGN TABLE IF EXISTS article;''')
cur.execute(f'''DROP USER MAPPING IF EXISTS FOR {LOCAL_DB_CREDENTIALS['user']} SERVER news;''')
cur.execute(f'''DROP SERVER IF EXISTS news CASCADE;''')
conn.commit()
cur.execute('''
SELECT table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');''')
r = cur.fetchall()
print(r)




