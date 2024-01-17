import psycopg2
from config import LOCAL_DB_CREDENTIALS


conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
"""fixing up urls and making the hash so that hash between article and submissions are comparable"""


def trim_url():
    cur = conn.cursor()
    cur.execute("SELECT id, url FROM article")
    rows = cur.fetchall()
    update_query = "UPDATE article SET url_trimmed = %s WHERE id = %s"
    updated_rows = []
    for row in rows:
        updated_value = clean_url(row[1])
        updated_rows.append((updated_value, row[0]))
    cur.executemany(update_query, updated_rows)
    conn.commit()
    cur.close()


def delete_duplicates_and_unique_constraint():
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM article''')
    r = cur.fetchone()
    print(r)
    cur.execute('''DELETE FROM article
                   WHERE id IN (
                    SELECT id FROM article
                    EXCEPT SELECT MIN(id) FROM article
                    GROUP BY url_trimmed
                    );''')
    cur.execute('''SELECT COUNT(*) FROM article''')
    r = cur.fetchone()
    print(r)
    cur.execute('''DELETE FROM article
                       WHERE id IN (
                        SELECT id FROM article
                        EXCEPT SELECT MIN(id) FROM article
                        GROUP BY url
                        );''')
    cur.execute('''SELECT COUNT(*) FROM article''')
    r = cur.fetchone()
    print(r)
    cur.execute('''ALTER TABLE submission DROP CONSTRAINT submission_url_key''')
    conn.commit()
    cur.close()


def add_remake_url_hash():
    cur = conn.cursor()
    cur.execute('''ALTER TABLE article DROP COLUMN url_hash''')
    cur.execute(
        '''ALTER TABLE article ADD COLUMN url_hash VARCHAR(32) GENERATED ALWAYS AS (MD5(url_trimmed)) STORED UNIQUE''')
    conn.commit()
    cur.close()


def clean_url(url):
    # TODO: cur.execute('''UPDATE article SET url_trimmed = split_part(url,'?', 1)''')
    # TODO: change 1 url in both tables, and see if the hash changes
    if url.startswith('http://'):
        url = url[7:]
    if url.startswith('https://'):
        url = url[8:]
    if url.startswith('www.'):
        url = url[4:]
    url = url.rstrip('/')
    return url