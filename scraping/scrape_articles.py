import re
import multiprocessing
import newspaper
from random import sample
from psycopg2.extras import RealDictCursor


def scrape_and_save_url_list(client, url_list):
    total_elements = len(url_list)
    for idx, url in enumerate(url_list):
        scrape_and_save_article(url, client)
        if idx % 100 == 0:
            print(f"{idx} completed. {(100*idx/total_elements):.2f}%")


def scrape_and_save_article(url, client):
    article = make_article(url)
    url_trimmed = clean_url(article.url)
    if client.url_trimmed_in_db(url_trimmed):
        return 0
    article = download_with_timelimit(article, timelimit=10)
    if article:
        parse_article(article)
        article_dict = make_article_dict(article=article, url_trimmed=url_trimmed)
        if article_dict['domain']:
            try:
                client.save_article(article_dict)
            except Exception as e:
                # for k in article_dict:
                #     print(k, article_dict[k], '\n')
                client.conn.rollback()
                print(article_dict['url'])
                print(e)


def make_article(url):
    if not url.startswith('http'):
        url = "https://" + url
    article = newspaper.Article(url=url)
    return article


def download_article(article):
    try:
        article.download()
    except:
        article = None
    return article


def download_with_timelimit(article, timelimit=5):
    pool = multiprocessing.Pool(processes=1)
    result = pool.apply_async(download_article, (article,))
    try:
        downloaded_article = result.get(timeout=timelimit)
    except multiprocessing.TimeoutError as e:
        downloaded_article = article
        downloaded_article.html = "timelimit error"
    pool.close()
    return downloaded_article


def parse_article(article):
    try:
        article.parse()
        article.nlp()
    except:
        pass


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


def make_article_dict(article, url_trimmed):
    article_dict = {}
    article_dict['url'] = article.url
    article_dict['url_trimmed'] = url_trimmed
    article_dict['domain'] = extract_domain(url_trimmed)
    article_dict['html'] = article.html.replace("\x00", "\uFFFD")
    article_dict['article_title'] = article.title
    article_dict['article_text'] = article.text
    article_dict['article_summary'] = article.summary
    if article.publish_date:
        article_dict['date_published'] = article.publish_date
    return article_dict


def query_url_list(conn, url_list):
    url_list = url_list[:50000]
    urls = sample(url_list, 100)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    for url in urls:
        article = make_article(url)
        url_trimmed = clean_url(article.url)
        cur.execute('''SELECT html, article_title, article_summary, article_text FROM article where url_trimmed = %s''',
                    (url_trimmed,))
        r = cur.fetchone()
        print(url_trimmed)
        print(r['article_title'])
        print(r['article_summary'])
        print(r['html'][:500])
        input('...')
    cur.close()
    conn.close()
