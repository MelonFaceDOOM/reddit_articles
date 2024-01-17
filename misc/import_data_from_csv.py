import csv
import json
from config import LOCAL_DB_CREDENTIALS
from db_client import establish_psql_connection, PsqlClient
from utilities import get_file_paths_from_folder

# Increase the field size limit
csv.field_size_limit(2147483647)

conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
client = PsqlClient(conn)
# client.drop_all_tables()
client.make_core_tables()

files = get_file_paths_from_folder('D:/work/projects/reddit_articles/results')
files = [f for f in files if "misinfo" not in f]


def main():
    chunksize = 10 ** 5
    for f in files[4:]:
        with open(f, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows_to_save = []
            row = next(reader)
            while True:
                try:
                    if len(rows_to_save) < chunksize:
                        rows_to_save.append(row)
                    else:
                        rows_to_save = [clean_data_dict(row) for row in rows_to_save]
                        client.save_many_submissions(rows_to_save)
                        rows_to_save = [row]
                    row = next(reader)
                except StopIteration:
                    if rows_to_save:
                        rows_to_save = [clean_data_dict(row) for row in rows_to_save]
                        client.save_many_submissions(rows_to_save)
                    print(f'file {f}finished')
                    break


def clean_data_dict(data_dict):
    boolean_keys = ['is_created_from_ads_ui', 'is_self', 'is_video', 'over_18',
                    'pinned', 'stickied']
    integer_keys = ['num_comments', 'num_crossposts', 'score', 'gilded']
    json_keys = ['media', 'gildings', 'all_awardings']
    bigint_keys = ['created_utc', 'author_created_utc']
    for bk in boolean_keys:
        if not data_dict[bk]:
            data_dict[bk] = False
    for ik in integer_keys:
        if not data_dict[ik]:
            data_dict[ik] = 0
    for jk in json_keys:
        if not data_dict[jk]:
            data_dict[jk] = None
        else:
            data_dict[jk] = json.dumps(data_dict[jk])
    for bik in bigint_keys:
        if not data_dict[bik]:
            data_dict[bik] = None
        # else:
        #     data_dict[bik] = int(data_dict[bik])
    if not data_dict['author_created_utc']:
        data_dict['author_created_utc'] = None
    if not data_dict['upvote_ratio']:
        data_dict['upvote_ratio'] = 0

    for key in data_dict:
        if type(data_dict[key]) == str:
            data_dict[key] = data_dict[key].replace('\x00', '')

    return data_dict


if __name__ == "__main__":
    main()
