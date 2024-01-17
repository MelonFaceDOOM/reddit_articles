import psycopg2
from config import LOCAL_DB_CREDENTIALS, NETWORK_DB_CREDENTIALS
from search_database_articles import save_submissions_and_comments_and_articles_in_time_period
from time import perf_counter


"""just some tests for network db connection"""


def main():
    save_folder = 'search_results/search_test_7'
    search_terms = ['monkeypox']
    sub_search_terms = []
    start_date = '2022-12-01'
    end_date = '2022-12-31'

    local_conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    # print_indices(local_conn)
    t1 = perf_counter()
    save_submissions_and_comments_and_articles_in_time_period(local_conn, save_folder, search_terms, sub_search_terms,
                                                              start_date, end_date)
    t2 = perf_counter()
    print(t2 - t1)
    local_conn.close()

    print('\n---------\n')
    save_folder = 'search_results/search_test_8'
    conn = psycopg2.connect(**NETWORK_DB_CREDENTIALS)
    # print_indices(conn)
    t1 = perf_counter()
    save_submissions_and_comments_and_articles_in_time_period(conn, save_folder, search_terms, sub_search_terms,
                                                              start_date, end_date)
    t2 = perf_counter()
    print(t2 - t1)
    conn.close()


def print_indices(conn):
    index_query = """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'public';  -- You can modify the condition based on your schema
    """
    cur = conn.cursor()
    cur.execute(index_query)
    indices = cur.fetchall()
    for index in indices:
        print(f"Index Name: {index[0]}, Definition: {index[1]}")
    cur.close()


if __name__ == "__main__":
    main()
