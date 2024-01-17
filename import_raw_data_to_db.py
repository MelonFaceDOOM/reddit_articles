import os
import json
import sys
import psycopg2
import time
from utilities import get_file_paths_from_folder
from raw_processing.decompress_zst import decompress_zstandard_to_folder
from parse import parse_chunks
from config import NETWORK_DB_CREDENTIALS
from db_client import PsqlClient

submission_data_keys = ["all_awardings", "author", "author_created_utc", "created_utc", "domain", "gilded", "gildings", "id", "is_created_from_ads_ui", "is_self", "is_video", "media", "num_comments", "num_crossposts", "over_18", "permalink", "pinned", "score", "stickied", "subreddit", "subreddit_id", "title", "upvote_ratio", "url", "url_overridden_by_dest"]
comment_data_keys = ["all_awardings", "author", "author_created_utc", "body", "created_utc", "gilded", "gildings", "id", "is_submitter", "link_id", "parent_id", "permalink", "score", "stickied", "subreddit", "subreddit_id", "subreddit_type", "total_awards_received"]
submissions_compressed_folder = "E:/data/reddit_data/submissions/compressed"
submissions_decompressed_folder = "E:/data/reddit_data/submissions/decompressed"
comments_compressed_folder = "E:/data/reddit_data/comments/compressed"
comments_decompressed_folder = "E:/data/reddit_data/comments/decompressed"

conn = psycopg2.connect(**NETWORK_DB_CREDENTIALS)
client = PsqlClient(conn)


def main():
    # uncomment to save raw data to db
    # save_submissions_data_to_db(first_file='RS_2023-02.zst')
    # save_comments_data_to_db(first_file='RC_2023-06.zst')
    pass  # just putting this here so linter doesn't red-underline the file


def save_submissions_data_to_db(first_file=None):
    compressed_files = get_file_paths_from_folder(submissions_compressed_folder)
    compressed_files.sort()
    if first_file:
        filenames = [os.path.basename(filepath) for filepath in compressed_files]
        index = filenames.index(first_file)
        compressed_files = compressed_files[index:]
    for compressed_file in compressed_files:
        decompress_zstandard_to_folder(compressed_file, submissions_decompressed_folder)
        decompressed_file = get_file_paths_from_folder(submissions_decompressed_folder)[0]
        save_all_submissions_in_decompressed_file(decompressed_file)
        with open('log_import_submissions.txt', 'w') as f:
            f.write(compressed_file)
        os.remove(decompressed_file)


def save_all_submissions_in_decompressed_file(decompressed_file):
    for chunk in parse_chunks(decompressed_file, chunksize=10_000):
        cleaned_submissions = []
        for submission in chunk:
            try:
                submission = clean_submission(submission)
                if submission['url']:
                    cleaned_submissions.append(submission)
            except Exception as e:
                print('failed to clean submission')
                for key in submission:
                    print(key, submission[key])
                print('------------------------\n\n')
                print(e)
                sys.exit()
        client.save_many_submissions(cleaned_submissions)


def save_comments_data_to_db(first_file=None):
    compressed_files = get_file_paths_from_folder(comments_compressed_folder)
    compressed_files.sort()
    if first_file:
        filenames = [os.path.basename(filepath) for filepath in compressed_files]
        index = filenames.index(first_file)
        compressed_files = compressed_files[index:]
    for compressed_file in compressed_files:
        decompress_zstandard_to_folder(compressed_file, comments_decompressed_folder)
        decompressed_file = get_file_paths_from_folder(comments_decompressed_folder)[0] #assumes folder is empty other than relevant file
        save_all_comments_in_decompressed_file(decompressed_file)
        with open('log_import_comments.txt', 'w') as f:
            f.write(compressed_file)
        os.remove(decompressed_file)


def save_all_comments_in_decompressed_file(decompressed_file):
    for chunk in parse_chunks(decompressed_file, chunksize=10000):
        cleaned_comments = []
        for comment in chunk:
            comment = clean_comment(comment)
            cleaned_comments.append(comment)
        client.save_many_comments(cleaned_comments)


def extract_data_from_parsed_file(parsed_file, data_keys):
    with open(parsed_file, 'r') as f:
        parsed_data = json.load(f)
    output_data = []
    for d in parsed_data:
        extracted_data = {}
        for data_key in data_keys:
            extracted_data[data_key] = extract_key_from_parsed_data(d, data_key)
        output_data.append(extracted_data)
    return output_data


def extract_key_from_parsed_data(parsed_data, data_key):
    if data_key in parsed_data:
        extracted_data = parsed_data[data_key]
    else:
        extracted_data = ''
    if not extracted_data:
        extracted_data = ''
    return extracted_data


def clean_submission(raw):
    data_dict = {}
    for key in submission_data_keys:
        if key in raw:
            data_dict[key] = raw[key]
        else:
            data_dict[key] = None

    boolean_keys = ['is_created_from_ads_ui', 'is_self', 'is_video', 'over_18',
                 'pinned', 'stickied']
    integer_keys = ['num_comments', 'num_crossposts', 'score', 'gilded']
    json_keys = ['media', 'gildings', 'all_awardings']
    bigint_keys = ['created_utc', 'author_created_utc']

    for bk in boolean_keys:
        if bk in data_dict:
            if not data_dict[bk]:
                data_dict[bk] = False
    for ik in integer_keys:
        if ik in data_dict:
            if not data_dict[ik]:
                data_dict[ik] = 0
    for jk in json_keys:
        if jk in data_dict:
            if not data_dict[jk]:
                data_dict[jk] = None
            else:
                data_dict[jk] = json.dumps(data_dict[jk])
    for bik in bigint_keys:
        if bik in data_dict:
            if not data_dict[bik]:
                data_dict[bik] = None
    if 'upvote_ratio' in data_dict:
        if not data_dict['upvote_ratio']:
            data_dict['upvote_ratio'] = 0
    if 'url' in data_dict:
        data_dict['url'] = clean_url(data_dict['url'])
    if 'author' in data_dict:
        # author can be "[deleted]" for some reason
        if type(data_dict['author']) != str:
            data_dict['author'] = None
    for key in data_dict:
        if type(data_dict[key]) == str:
            data_dict[key] = data_dict[key].replace('\x00', '')
    return data_dict


def clean_comment(raw):
    data_dict = {}
    for key in comment_data_keys:
        if key in raw:
            data_dict[key] = raw[key]
        else:
            data_dict[key] = None
    boolean_keys = ['is_submitter', 'stickied']
    integer_keys = ['score', 'gilded', 'total_awards_received']
    json_keys = ['media', 'gildings', 'all_awardings']
    bigint_keys = ['created_utc', 'author_created_utc']
    for bk in boolean_keys:
        if bk in data_dict:
            if not data_dict[bk]:
                data_dict[bk] = False
    for ik in integer_keys:
        if ik in data_dict:
            if not data_dict[ik]:
                data_dict[ik] = 0
    for jk in json_keys:
        if jk in data_dict:
            if not data_dict[jk]:
                data_dict[jk] = None
            else:
                data_dict[jk] = json.dumps(data_dict[jk])
    for bik in bigint_keys:
        if bik in data_dict:
            if not data_dict[bik]:
                data_dict[bik] = None

    # subreddit_id and link_id seem to always have an unnecessary suffix that will ruin joins
    if not data_dict['subreddit_id'].startswith('t5_'):
        raise ValueError(f"subreddit_id {data_dict['subreddit_id']} doesn't start with t5_")
    else:
        data_dict['subreddit_id'] = data_dict['subreddit_id'][3:]
    if not data_dict['link_id'].startswith('t3_'):
        raise ValueError(f"link_id {data_dict['link_id']} doesn't start with t3_")
    else:
        data_dict['link_id'] = data_dict['link_id'][3:]

    if 'author' in data_dict:
        # author can be "[deleted]"
        if type(data_dict['author']) != str:
            data_dict['author'] = None
    for key in data_dict:
        if type(data_dict[key]) == str:
            data_dict[key] = data_dict[key].replace('\x00', '')
    return data_dict


def clean_url(url):
    if not url:
        return None
    if url.startswith('http://'):
        url = url[7:]
    if url.startswith('https://'):
        url = url[8:]
    if url.startswith('www.'):
        url = url[4:]
    url = url.rstrip('/')
    return url


def print_all_indices(conn):
    cur = conn.cursor()
    cur.execute('''select *
                   from pg_indexes
                   where tablename not like 'pg%';''')
    r = cur.fetchall()
    print(r)
    cur.close()


def chunk_timetest():
    """warning: will drop submissions table"""
    total_submissions = 100000
    chunksizes = [10, 100, 1000, 10000]
    trials = 3
    decompressed_file = get_file_paths_from_folder(submissions_decompressed_folder)[0]

    for trial in range(trials):
        # client.drop_submissions()
        t1 = time.perf_counter()
        for chunk in parse_chunks(decompressed_file, chunksize=1, total=total_submissions):
            submission = clean_submission(chunk[0])
            client.save_submission(submission)
        t2 = time.perf_counter()
        print(f"1 at a time, trial {trial}: {t2 - t1:.2f}")
        # client.drop_submissions()
        time.sleep(10)

        for chunksize in chunksizes:
            t1 = time.perf_counter()
            for chunk in parse_chunks(decompressed_file, chunksize=chunksize, total=total_submissions):
                cleaned_submissions = []
                for submission in chunk:
                    cleaned_submissions.append(clean_submission(submission))
                client.save_many_submissions(cleaned_submissions)
            t2 = time.perf_counter()
            print(f"{chunksize} at a time, trial {trial}: {t2 - t1:.2f}")
            # client.drop_submissions()
            time.sleep(10)


def join_timetest():
    t1 = time.perf_counter()
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM article JOIN submission using(url_hash)''')
    r = cur.fetchone()
    print(r)
    t2 = time.perf_counter()
    print(t2-t1, "time for count")

    t1 = time.perf_counter()
    cur = conn.cursor()
    cur.execute('''SELECT * FROM article JOIN submission using(url_hash)''')
    r = cur.fetchone()
    print(r)
    t2 = time.perf_counter()
    print(t2 - t1, "time for full select")


def comment_join_timetest():
    t1 = time.perf_counter()
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM submission JOIN comment ON submission.id = comment.link_id''')
    r = cur.fetchone()
    print(r)
    t2 = time.perf_counter()
    print(t2 - t1, "time for count")


def comment_article_submission_join_timetest():
    t1 = time.perf_counter()
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) 
                    FROM
                        submission s
                    JOIN
                        article a ON s.url_hash = a.url_hash
                    JOIN
                        comment c ON s.id = c.link_id
                    ''')
    r = cur.fetchone()
    print(r)
    t2 = time.perf_counter()
    print(t2 - t1, "time for count")


def print_indices(cur):
    cur.execute('''SELECT schemaname, tablename, indexname
    FROM pg_indexes
    ORDER BY schemaname, tablename, indexname;''')
    r = cur.fetchall()
    for i in r:
        print(i)


if __name__ == "__main__":
    main()
