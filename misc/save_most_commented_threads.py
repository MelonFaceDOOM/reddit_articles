"""select threads with most comments. I scraped all these urls later on"""
import csv
from psycopg2.extras import RealDictCursor
from config import LOCAL_DB_CREDENTIALS
import psycopg2

conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
cur = conn.cursor(cursor_factory=RealDictCursor)
cur.execute('''SELECT * FROM submission ORDER BY num_comments DESC LIMIT 500000''')
r = cur.fetchall()


csv_filename = 'top_commented_submissions.csv'
with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = r[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in r:
        writer.writerow(row)
