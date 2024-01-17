from psycopg2.extras import execute_values, RealDictCursor

"""I initially imagined that I would write a bunch of template sql and put them in methods in this class,
   but it turns out pretty much every use case involves just writing new sql from scratch
   
   This mean that this is class is basically used to set up the db and to save data to it."""


class PsqlClient:
    def __init__(self, conn):
        self.conn = conn
        
    def make_core_tables(self):
        cur = self.conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS submission (
                        db_id SERIAL PRIMARY KEY,
                        id TEXT UNIQUE NOT NULL,
                        url TEXT NOT NULL,
                        url_hash VARCHAR(32) GENERATED ALWAYS AS (MD5(url)) STORED UNIQUE,
                        domain TEXT NOT NULL,
                        title TEXT NOT NULL,
                        permalink TEXT,
                        created_utc BIGINT NOT NULL,
                        url_overridden_by_dest text,
                        subreddit_id TEXT NOT NULL,
                        subreddit TEXT NOT NULL,
                        author TEXT NOT NULL,
                        author_created_utc BIGINT,
                        upvote_ratio NUMERIC NOT NULL,
                        score INTEGER NOT NULL,
                        gilded INTEGER NOT NULL,
                        num_comments INTEGER NOT NULL,
                        num_crossposts INTEGER NOT NULL,
                        pinned boolean NOT NULL,
                        stickied boolean NOT NULL,
                        over_18 boolean NOT NULL,                        
                        is_created_from_ads_ui boolean NOT NULL,
                        is_self boolean NOT NULL,
                        is_video boolean NOT NULL,
                        media jsonb,
                        gildings jsonb,
                        all_awardings jsonb
                    );""")

        cur.execute("""CREATE TABLE IF NOT EXISTS comment (
                        id TEXT PRIMARY KEY,
                        parent_id TEXT, 
                        link_id TEXT NOT NULL,
                        body TEXT NOT NULL,
                        permalink TEXT NOT NULL,
                        created_utc BIGINT NOT NULL,
                        subreddit_id TEXT NOT NULL,
                        subreddit_type TEXT,
                        total_awards_received INTEGER NOT NULL,
                        subreddit TEXT NOT NULL,
                        author TEXT NOT NULL,
                        author_created_utc BIGINT,
                        score INTEGER NOT NULL,
                        gilded INTEGER NOT NULL,
                        stickied boolean NOT NULL,
                        is_submitter boolean NOT NULL,
                        gildings jsonb,
                        all_awardings jsonb
                    );""")

        cur.execute('''CREATE TABLE IF NOT EXISTS article
                         (id SERIAL PRIMARY KEY,
                         date_entered TIMESTAMP DEFAULT current_timestamp,
                         date_published TIMESTAMP,
                         url TEXT NOT NULL,
                         url_trimmed TEXT NOT NULL,
                         url_hash VARCHAR(32) GENERATED ALWAYS AS (MD5(url_trimmed)) STORED UNIQUE,
                         domain TEXT NOT NULL,  
                         html TEXT NOT NULL,
                         article_title TEXT NOT NULL,
                         article_text TEXT NOT NULL,
                         article_summary TEXT NOT NULL,
                         title_ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', article_title)) STORED,
                         text_ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', article_text)) STORED,
                         summary_ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', article_summary)) STORED)''')
        self.conn.commit()
        cur.close()

    # def drop_all_tables(self):
    #     cur = self.conn.cursor()
    #     cur.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public'"
    #                 "ORDER BY table_schema,table_name")
    #     rows = cur.fetchall()
    #     for row in rows:
    #         print("dropping table: ", row[1])
    #         cur.execute("drop table " + row[1] + " cascade")
    #     self.conn.commit()
    #     cur.close()

    # def drop_submissions(self):
    #     cur = self.conn.cursor()
    #     cur.execute("""DELETE FROM submission""")
    #     self.conn.commit()
    #     cur.close()
    
    # def drop_comments(self):
    #     cur = self.conn.cursor()
    #     cur.execute("""DELETE FROM comment""")
    #     self.conn.commit()
    #     cur.close()
    
    def save_submission(self, submission):
        cur = self.conn.cursor()
        cols = submission.keys()
        cols_string = ", ".join(cols)
        value_placeholders = ','.join(['%s'] * len(submission))
        values = [submission[col] for col in cols]
        query = f"INSERT INTO submission ({cols_string}) VALUES ({value_placeholders}) ON CONFLICT DO NOTHING"
        cur.execute(query, values)
        self.conn.commit()
        cur.close()

    def save_many_submissions(self, submissions):
        cur = self.conn.cursor()
        cols = submissions[0].keys()
        cols_string = ", ".join(cols)
        values = [[submission[col] for col in cols] for submission in submissions]
        query = f"INSERT INTO submission ({cols_string}) VALUES %s ON CONFLICT DO NOTHING"
        execute_values(cur, query, values)
        self.conn.commit()
        cur.close()

    def save_comment(self, comment):
        cur = self.conn.cursor()
        cols = comment.keys()
        cols_string = ", ".join(cols)
        value_placeholders = ','.join(['%s'] * len(comment))
        values = [comment[col] for col in cols]
        query = f"INSERT INTO comment ({cols_string}) VALUES ({value_placeholders}) ON CONFLICT DO NOTHING"
        cur.execute(query, values)
        self.conn.commit()
        cur.close()

    def save_many_comments(self, comments):
        cur = self.conn.cursor()
        cols = comments[0].keys()
        cols_string = ", ".join(cols)
        values = [[comment[col] for col in cols] for comment in comments]
        query = f"INSERT INTO comment ({cols_string}) VALUES %s ON CONFLICT DO NOTHING"
        execute_values(cur, query, values)
        self.conn.commit()
        cur.close()

    def url_trimmed_in_db(self, url):
        """checks url_trimmed"""
        cur = self.conn.cursor()
        cur.execute('''SELECT count(*) FROM article WHERE url_trimmed = %s''', (url,))
        r = cur.fetchone()[0]
        if r > 0:
            url_present = True
        else:
            url_present = False
        cur.close()
        return url_present

    def save_article(self, article):
        cur = self.conn.cursor()
        cols = article.keys()
        cols_string = ", ".join(cols)
        value_placeholders = ','.join(['%s'] * len(article))
        values = [article[col] for col in cols]
        query = f"INSERT INTO article ({cols_string}) VALUES ({value_placeholders}) ON CONFLICT DO NOTHING"
        # query = f"INSERT INTO article ({cols_string}) VALUES ({value_placeholders})"
        cur.execute(query, values)
        self.conn.commit()
        cur.close()

    def make_indices(self):
        cur = self.conn.cursor()
        cur.execute('''CREATE INDEX IF NOT EXISTS submission_url_hash_key' ON submission(url_hash)''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_comment_link_id ON comment(link_id)''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_article_url_trimmed ON article(url_trimmed)''')
        self.conn.commit()

    def search_in_articles(self, column, search_words):
        search_string = " ".join(search_words)
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f'''SELECT *
                        FROM article
                        WHERE {column} @@ plainto_tsquery('english', '{search_string}');''')
        results = cur.fetchall()
        cur.close()
        return results
#
#     @staticmethod
#     def find_or_insert_paper(cur, paper_url):
#         paper_url = paper_url.lower().strip()
#         cur.execute('''SELECT id FROM paper WHERE url = %s''', (paper_url,))
#         paper_id = cur.fetchone()
#         if not paper_id:
#             cur.execute('''INSERT INTO paper (url) VALUES (%s) ON CONFLICT DO NOTHING''', (paper_url,))
#             cur.execute('''SELECT id FROM paper WHERE url = %s''', (paper_url,))
#             paper_id = cur.fetchone()
#         return paper_id[0]
#
#     @staticmethod
#     def find_or_insert_authors(cur, authors):
#         author_ids = []
#         for author in authors:
#             author = author.lower().strip()
#             cur.execute('''SELECT id FROM author WHERE name = %s''', (author,))
#             author_id = cur.fetchone()
#             if not author_id:
#                 cur.execute('''INSERT INTO author (name) VALUES (%s) ON CONFLICT DO NOTHING''', (author,))
#                 cur.execute('''SELECT id FROM author WHERE name = %s''', (author,))
#                 author_id = cur.fetchone()
#             author_ids.append(author_id[0])
#         return author_ids
#
#     @staticmethod
#     def find_or_insert_article(cur, url, domain, html, title, text, summary, date_published, paper_id):
#         url = url.lower().strip()
#         html = html.lower().strip()
#         title = title.lower().strip()
#         text = text.lower().strip()
#         cur.execute('''SELECT id FROM article WHERE url = %s''', (url,))
#         article_id = cur.fetchone()
#         retries = 0
#         # for some reason it sometimes (1/10,000+) takes more than one try to insert the article
#         while not article_id and retries < 3:
#             cur.execute('''INSERT INTO article (url, domain, html, article_title, article_text, article_summary,
#                         date_published, paper_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING''',
#                         (url, domain, html, title, text, summary, date_published, paper_id))
#             cur.execute('''SELECT id FROM article WHERE url = %s''', (url,))
#             article_id = cur.fetchone()
#         return article_id[0]
#
#     @staticmethod
#     def insert_article_author(cur, article_id, author_id):
#         cur.execute('''INSERT INTO article_author (article_id, author_id) VALUES (%s, %s) ON CONFLICT DO NOTHING''',
#                     (article_id, author_id))
#
#     def insert_articles(self, articles):
#         with self.conn.cursor() as cur:
#             for article in articles:
#                 try:
#                     paper_id = self.find_or_insert_paper(cur, article['paper_url'])
#                     author_ids = self.find_or_insert_authors(cur, article['authors'])
#                     article_id = self.find_or_insert_article(cur, url=article['url'],
#                                                         domain=article['paper_url'],
#                                                         html=article['html'],
#                                                         title=article['title'],
#                                                         text=article['text'],
#                                                         summary=article['summary'],
#                                                         date_published=article['date_published'],
#                                                         paper_id=paper_id)
#                     for author_id in author_ids:
#                         self.insert_article_author(cur, article_id, author_id)
#                     self.conn.commit()
#                 except:
#                     pass
#
