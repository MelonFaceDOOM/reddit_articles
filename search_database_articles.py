import time
import os
import psycopg2
import pandas as pd
from datetime import datetime, timezone
from psycopg2.extras import RealDictCursor
from config import NETWORK_DB_CREDENTIALS


def main():
    """finds all articles where search_terms are mentioned
    articles where sub_search_terms are mentioned will be labeled
    joins list of article urls to submissions
    joins submission ids to comments
    save articles, comments, and submissions in separate csv files to specified folder"""
    save_folder = 'search_results/search_test_6'
    search_terms = ['covid', 'coronavirus']
    sub_search_terms = ['myocarditis', 'bill gates', 'depopulate', 'lab leak', 'bioweapon', 'bio weapon', '5g', 'microchip', 'event 201', 'flu-like', 'exaggerated flu']
    start_date = '2020-01-01'
    end_date = '2022-12-31'
    if not os.path.exists(save_folder):
        os.mkdir(save_folder)
    conn = psycopg2.connect(**NETWORK_DB_CREDENTIALS)
    save_submissions_and_comments_and_articles_in_time_period(conn, save_folder, search_terms, sub_search_terms, start_date, end_date)


def save_submissions_and_comments_and_articles_in_time_period(conn, save_folder, search_terms, sub_search_terms, start_date, end_date):
    article_with_keywords = search_in_articles(conn, "text_ts", search_terms)
    article_url_hashes = [i['url_hash'] for i in article_with_keywords]
    submissions_in_date_range = search_submission_url_hashes_in_time_period(conn, article_url_hashes, start_date, end_date)
    submission_url_hashes_in_date_range = [i['url_hash'] for i in submissions_in_date_range]
    # articles_in_date_range = search_url_hashes_in_articles(conn, submission_url_hashes_in_date_range)
    submission_ids_in_date_range = [i['id'] for i in submissions_in_date_range]
    comments_in_date_range = search_submission_id_in_comments(conn, submission_ids_in_date_range)

    df = pd.DataFrame(submissions_in_date_range)
    df.to_csv(os.path.join(save_folder, 'submissions.csv'), index=False)
    df = pd.DataFrame(comments_in_date_range)
    df.to_csv(os.path.join(save_folder, 'comments.csv'), index=False)
    create_temp_article_table(conn, submission_url_hashes_in_date_range)
    articles = poo(conn, sub_search_terms)
    df = pd.DataFrame(articles)
    df.to_csv(os.path.join(save_folder, 'articles.csv'), index=False)


def poo(conn, search_terms):
    ids_and_tags = {}
    for search_term in search_terms:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f'''SELECT id
                        FROM article_results
                        WHERE text_ts @@ phraseto_tsquery ('english', '{search_term}');''')
        results = cur.fetchall()
        cur.close()
        for r in results:
            if r['id'] in ids_and_tags:
                ids_and_tags[r['id']].append(search_term)
            else:
                ids_and_tags[r['id']] = [search_term]
    ids_and_tags = [[i, ",".join(ids_and_tags[i])] for i in ids_and_tags]
    cur = conn.cursor()
    update_query = '''UPDATE article_results SET tags = %s WHERE id = %s'''
    for data in ids_and_tags:
        cur.execute(update_query, (data[1], data[0]))
    cur.close()
    conn.commit()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f'''SELECT id, tags, date_published, url_trimmed, url_hash, domain,
                    article_text, article_title, article_summary
                    FROM article_results''')
    r = cur.fetchall()
    cur.close()
    return r


def search_submission_url_hashes_in_time_period(conn, url_hashes, start_date, end_date):
    """returns ids where url_hashes matched"""
    start_utc = convert_date_to_utc(start_date)
    end_utc = convert_date_to_utc(end_date)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f'''SELECT id, url, url_hash, domain, title, permalink, created_utc, subreddit, upvote_ratio, score, gilded,
                    num_comments, num_crossposts, is_self, is_video, gildings, all_awardings 
                    FROM submission WHERE url_hash in %s AND created_utc BETWEEN {start_utc} AND {end_utc}''',
                    (tuple(url_hashes),))
    r = cur.fetchall()
    cur.close()
    return r


def convert_date_to_utc(date_string):
    """takes date_string in y-m-d format"""
    parsed_date = datetime.strptime(date_string, "%Y-%m-%d")
    timestamp = parsed_date.replace(tzinfo=timezone.utc).timestamp()
    return timestamp


def search_in_articles(conn, column, search_terms):
    """search_terms is a list of terms that will be joined by an OR operator
    returns ids and url_hashes"""
    search_string = " | ".join(search_terms)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f'''SELECT id, url_hash
                    FROM article
                    WHERE {column} @@ to_tsquery('english', '{search_string}');''')
    results = cur.fetchall()
    cur.close()
    return results


def create_temp_article_table(conn, url_hashes):
    cur = conn.cursor()
    cur.execute('''CREATE TEMP TABLE article_results AS
                   SELECT * FROM article
                   WHERE url_hash in %s''',
                   (tuple(url_hashes),))
    cur.execute('''ALTER TABLE article_results ADD tags TEXT''')
    cur.execute('''SELECT COUNT(*) FROM article_results''')
    r = cur.fetchone()
    print(r)
    conn.commit()
    cur.close()


def search_url_hashes_in_articles(conn, url_hashes):
    """repalced by create_temp_article_table()"""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f'''SELECT id, date_published, url_trimmed, url_hash, domain,
                    html, article_text, article_title, article_summary, text_ts, title_ts, summary_ts
                    FROM article WHERE url_hash in %s''',
                    (tuple(url_hashes),))
    r = cur.fetchall()
    cur.close()
    return r


def search_submission_id_in_comments(conn, submission_ids):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('''SELECT * FROM comment WHERE link_id in %s''', (tuple(submission_ids),))
    r = cur.fetchall()
    cur.close()
    return r


def trim_and_update_limit(table, column, limit, conn):
    # alter query didn't work because it edits a column that is used to generate another column
    # just trim all data before entering it rather than relying on a db constraint
    # also add the date constraint logic to that same cleaning func (date > 1900)
    cur = conn.cursor()
    update_query = f'''UPDATE {table} SET {column} = LEFT({column}, {limit})'''
    cur.execute(update_query)
    # alter_query = f'''ALTER TABLE {table} ALTER COLUMN {column} TYPE VARCHAR({limit})'''
    # cur.execute(alter_query)
    conn.commit()
    cur.close()


def update_dates(conn):
    cur = conn.cursor()
    cur.execute('''UPDATE article SET date_published = NULL WHERE date_published < '1900-01-01'::timestamp;''')
    conn.commit()
    cur.close()


def check_articles(conn):
    """(1183065,)
    (251859,)
    (1949,)
    """
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM article''')
    r = cur.fetchone()
    print(r)
    cur.execute('''SELECT COUNT(*) FROM article WHERE length(article_text)<100''')
    r = cur.fetchone()
    print(r)
    cur.execute('''SELECT COUNT(*) FROM article WHERE html = %s''', ("timelimit error",))
    r = cur.fetchone()
    print(r)


def join3_and_save(conn):
    save_article_comments(conn)
    # df = pd.read_csv('comments.csv')
    # submission_id_list = df['submission_id'].unique().tolist()  # TODO: see what column name actually shows up as
    # save_submission_from_id_list(conn, submission_id_list)
    # article_id_list = df['article_id'].unique().tolist()
    # save_articles_from_id_list(conn, article_id_list)


def save_submission_from_id_list(conn, submission_id_list):
    cur = conn.cursor()
    cur.execute('''SELECT * FROM article WHERE id in %s''', (submission_id_list,))
    r = cur.fetchall()
    df = pd.DataFrame(r)
    df.to_csv('submissions.csv', index=False)


def save_articles_from_id_list(conn, article_id_list):
    cur = conn.cursor()
    cur.execute('''SELECT * FROM article WHERE id in %s''', (article_id_list,))
    r = cur.fetchall()
    df = pd.DataFrame(r)
    df.to_csv('articles.csv', index=False)


def save_top_commented_thread(conn):
    results = get_top_commented_threads(conn, 10_000_000)
    df = pd.DataFrame(results)
    df.to_csv('top_commented_10M.csv', index=False)


def save_article_comments(conn):
    """finds all reddit threads where a scraped article exists. saves comments, thread_id, and article_id"""
    results = comment_submission_article_join(conn)
    df = pd.DataFrame(results)
    df.to_csv('comments.csv', index=False)
    # df.to_csv('articles.csv', index=False)


def submission_article_jointest(conn):
    t1 = time.perf_counter()
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM article JOIN submission using(url_hash)''')
    r = cur.fetchone()
    print(r, "results from submission article join")
    t2 = time.perf_counter()
    print(t2-t1, "time for submission article join")
    cur.close()


def submission_comment_jointest(conn):
    """this will join all 4.5 billion comments"""
    t1 = time.perf_counter()
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM submission JOIN comment ON submission.id = comment.link_id''')
    r = cur.fetchone()
    print(r, "results from submission comment join")
    t2 = time.perf_counter()
    print(t2 - t1, "time for submission comment join")
    cur.close()


def comment_submission_article_join(conn):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    t1 = time.perf_counter()
    # cur.execute('''SELECT submission.id, article.id, article.url, article.url_trimmed, article.domain, article.article_title, article.article_text, article.article_summary
    #                FROM article
    #                JOIN submission
    #                ON article.url_hash = submission.url_hash
    #                ''')
    cur.execute('''SELECT submission.id, article.id, comment.id
                   FROM article
                   JOIN submission
                     ON article.url_hash = submission.url_hash
                   JOIN comment
                     ON submission.id = comment.link_id''')
    r = cur.fetchall()
    t2 = time.perf_counter()
    print(t2 - t1, "time for three way join")
    cur.close()
    return r


def get_top_commented_threads(conn, thread_count):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    t1 = time.perf_counter()
    cur.execute('''SELECT url FROM submission 
                   WHERE is_self = False
                   ORDER BY num_comments DESC LIMIT %s''', (thread_count,))
    r = cur.fetchall()
    t2 = time.perf_counter()
    print(t2 - t1, "time for most commented thread select")
    cur.close()
    return r


if __name__ == "__main__":
    main()
