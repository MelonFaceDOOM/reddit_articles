import pandas as pd
import re
import time
import newspaper
import psycopg2
from db_client import PsqlClient
from config import LOCAL_DB_CREDENTIALS

"""this code is mostly deprecated by scrape_articles.py"""


articles_file = 'D:/news_scraping/articles_temp/new_articles.csv'
    
    
def main():
    insert_each_article()


def insert_each_article():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    df = pd.read_csv(articles_file)
    article_count = len(df)
    attempts = 0
    html_error = 0
    article_exception = 0
    no_domain = 0
    df = df.drop_duplicates(subset=['url'])
    articles_processed = 0
    t1 = time.perf_counter()
    not_in_db_count = 0 
    for index, row in df.iterrows():
        if row['url'].startswith('http'):
            url = row['url']
        else:
            url = "https://" + row['url']
        article = newspaper.Article(url=url)
        url_trimmed = clean_url(article.url)
        if not url_trimmed_in_db(url_trimmed, conn):
            not_in_db_count += 1
            html = None
            if not pd.isnull(row['html']):
                if len(row['html']) > 200:
                    html = row['html']
            if html:
                article.set_html(html)
            else:
                try:
                    article.download()
                except:
                    article.html = "error"
            domain = extract_domain(url_trimmed)
            if domain and not article.html == "error":
                try:
                    article.parse()
                    article.nlp()
                    article_dict = clean_article(article, url_trimmed, domain)
                    client.save_article(article_dict)
                    attempts += 1
                except newspaper.article.ArticleException as e:
                    # print(row['url'])
                    # print(e)
                    article_exception += 1
                    # if article_exception > 50:
                        # sys.exit()
            if not domain:
                no_domain += 1
            if article.html == "error":
                html_error += 1
                
        articles_processed += 1
        if articles_processed % 1000 == 0:
            t2 = time.perf_counter()
            time_remaining = (t2-t1)*((article_count-articles_processed)/articles_processed)
            print(f"{articles_processed} / {article_count} -- {t2-t1:.0f}s elapsed. "
                  f"{time_remaining:.0f}s remaining")
            print("not in db:", not_in_db_count)
            print("html_error:", html_error)
            print("article_exception:", article_exception)
            print("no_domain:", no_domain)
            print("insert attempts:", attempts)
                
    
def url_trimmed_in_db(url, conn):
    cur = conn.cursor()
    cur.execute('''SELECT count(*) FROM article WHERE url_trimmed = %s''', (url,))
    r = cur.fetchone()[0]
    if r > 0:
        url_present = True
    else:
        url_present = False
    cur.close()
    return url_present
    
            
def clean_url(url):
    if not url:
        return None
    if url.startswith('http://'):
        url = url[7:]
    if url.startswith('https://'):
        url = url[8:]
    if url.startswith('www.'):
        url = url[4:]
    url = url.replace('/./', '/')
    url = url.strip('/: ')    
    return url    
    
    
def extract_domain(url):
    pattern = '(?:https?:\/\/)?(?:www\.)?((?:[\w-]+\.)+(?:\w+)+).*'
    match = re.match(pattern, url)
    if not match:
        return None
    return match.group(1)
    
    
def clean_article(article, url_trimmed, domain):
    article_dict = {}
    article_dict['url'] = article.url
    article_dict['url_trimmed'] = url_trimmed
    article_dict['domain'] = domain
    article_dict['html'] = article.html.replace("\x00", "\uFFFD")
    article_dict['article_title'] = article.title
    article_dict['article_text'] = article.text
    article_dict['article_summary'] = article.summary
    if article.publish_date:
        article_dict['date_published'] = article.publish_date
    return article_dict
              

def url_in_db(url, conn):
    cur = conn.cursor()
    cur.execute('''SELECT count(*) FROM article WHERE url = %s''', (url,))
    r = cur.fetchone()[0]
    if r > 0:
        url_present = True
    else:
        url_present = False
    cur.close()
    return url_present

    
def print_table_columns(conn):
    cur = conn.cursor()
    cur.execute('''SELECT
                    column_name,
                    data_type
                FROM
                    information_schema.columns
                WHERE
                    table_name = %s;''', ('article',))
    columns = cur.fetchall()
    for c in columns:
        print(c)
    cur.close()


if __name__ == "__main__":
    main()
