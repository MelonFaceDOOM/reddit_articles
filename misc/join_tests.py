import time
import psycopg2
from db_client import PsqlClient
from config import LOCAL_DB_CREDENTIALS


def main():
    # trim_url_more()
    # # test1()
    # make_index()
    # test1()
    # full_test()
    # # add_new_col_to_foreign_db()
    join_test()


def join_test():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM submissions a
                   JOIN article b
                   ON a.url_trimmed = b.url_trimmed''')
    output = cur.fetchone()
    print(output)
    cur.close()


def make_index():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    client.make_indices()
    print('index made')



def add_new_col_to_foreign_db():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    try:
        cur.execute('''SELECT url_trimmed from article limit 10''')
    except:
        conn.rollback()
        print('no table')
    cur.execute('''ALTER FOREIGN TABLE article ADD COLUMN url_trimmed text;''')
    try:
        cur.execute('''SELECT url_trimmed from article limit 10''')
        print('yes table')
    except:
        conn.rollback()
        print('still not table')
    conn.commit()
    cur.close()


def test1():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    t1 = time.perf_counter()

    cur.execute('''CREATE TEMPORARY TABLE sampled_article AS
                   SELECT *
                   FROM article
                   LIMIT 10000 OFFSET 0''')
    t2 = time.perf_counter()
    print(f"{t2 - t1:.2f}", "time to limit articles to 10000")

    cur.execute('''CREATE TEMPORARY TABLE sampled_submissions AS
                   SELECT *
                   FROM submissions
                   TABLESAMPLE BERNOULLI (0.1) REPEATABLE (123)''')

    t3 = time.perf_counter()
    print(f"{t3 - t2:.2f}", "time to sample submissions to 0.1%")

    cur.execute('''SELECT *
                   FROM sampled_submissions a
                   JOIN sampled_article b
                   ON a.url_trimmed = b.url_trimmed;''')
    t4 = time.perf_counter()
    print(f"{t4 - t3:.2f}", "time to join submissions sample on article sample")

    cur.execute('''SELECT *
                       FROM sampled_submissions a
                       JOIN article b
                       ON a.url_trimmed = b.url_trimmed;''')
    t5 = time.perf_counter()
    print(f"{t5 - t4:.2f}", "time to join submission sample on full article")

    r = cur.fetchall()
    print(len(r))
    cur.close()


def full_test():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()

    cur.execute('''CREATE TEMPORARY TABLE sampled_article AS
                   SELECT *
                   FROM article
                   LIMIT 10000 OFFSET 0''')

    t1 = time.perf_counter()

    cur.execute('''CREATE TEMPORARY TABLE sampled_submissions1 AS
                   SELECT *
                   FROM submissions
                   TABLESAMPLE BERNOULLI (0.3) REPEATABLE (123)''')

    t2 = time.perf_counter()
    print(f"{t2 - t1:.2f}", "time to sample submissions to 0.3%")

    cur.execute('''CREATE TEMPORARY TABLE sampled_submissions2 AS
                   SELECT *
                   FROM submissions
                   TABLESAMPLE BERNOULLI (0.9) REPEATABLE (123)''')

    t3 = time.perf_counter()
    print(f"{t3 - t2:.2f}", "time to sample submissions to 0.9%")

    cur.execute('''CREATE TEMPORARY TABLE sampled_submissions3 AS
                   SELECT *
                   FROM submissions
                   TABLESAMPLE SYSTEM (1)''')

    t4 = time.perf_counter()
    print(f"{t4 - t3:.2f}", "time to sample submissions to 1% using system")

    cur.execute('''SELECT *
                   FROM sampled_submissions1 a
                   JOIN sampled_article b
                   ON a.url_trimmed = b.url_trimmed;''')
    t5 = time.perf_counter()
    print(f"{t5 - t4:.2f}", "time to join submissions 0.3% to article sample")

    cur.execute('''SELECT *
                   FROM sampled_submissions2 a
                   JOIN sampled_article b
                   ON a.url_trimmed = b.url_trimmed;''')
    t6 = time.perf_counter()
    print(f"{t6 - t5:.2f}", "time to join submissions 0.9% to article sample")

    r = cur.fetchall()
    print(len(r))
    cur.close()


def add_url_trimmed_column():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute('''ALTER TABLE submissions
                   ADD COLUMN url_trimmed TEXT;''')
    cur.execute('''UPDATE submissions
                   SET url_trimmed = TRIM(TRAILING '/' FROM url);''')
    conn.commit()


def trim_url_more():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute('''UPDATE submissions
                   SET url_trimmed = SUBSTRING(url_trimmed FROM 1 FOR 100);''')
    conn.commit()
    cur.close()


if __name__ == "__main__":
    main()
