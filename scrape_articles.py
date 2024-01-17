import psycopg2
from scraping.scrape_articles import scrape_and_save_url_list
from config import LOCAL_DB_CREDENTIALS
from db_client import PsqlClient


with open('urls_to_scrape_extensions_removed.txt', 'r', encoding='utf-8') as f:
    list_of_articles_to_scrape = f.read().split('\n')
    list_of_articles_to_scrape = list_of_articles_to_scrape[189000:]


def main():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    scrape_and_save_url_list(client, list_of_articles_to_scrape)


if __name__ == "__main__":
    main()
