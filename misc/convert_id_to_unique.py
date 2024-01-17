import psycopg2
from config import LOCAL_DB_CREDENTIALS

"""had to do some stuff to convert id (reddit id string) column to unique"""


def main():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    # cur.execute("""SELECT COUNT(*) FROM comment
    #                WHERE created_utc > 1622379661
    #                AND created_utc < 1625144461""")
    # r = cur.fetchall()
    # print(r)

    # cur.execute('''delete
    #                from comment a
    #                using comment b
    #                where a.id = b.id
    #                and a.db_id > b.db_id''')
    # cur.execute('''ALTER TABLE comment DROP CONSTRAINT comment_pkey''')
    # cur.execute('''ALTER TABLE comment ADD PRIMARY KEY (id)''')
    # cur.execute('''ALTER TABLE comment DROP COLUMN db_id''')
    # conn.commit()

#     cur.execute('''SELECT conname AS constraint_name, conrelid::regclass AS table_name, a.attname AS column_name
# FROM pg_constraint c
# JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
# WHERE c.contype = 'p';''')
#     r=cur.fetchall()
#     for i in r:
#         print(i)
#     print_indices(cur)


def print_indices(cur):
    cur.execute('''SELECT schemaname, tablename, indexname
    FROM pg_indexes
    ORDER BY schemaname, tablename, indexname;''')
    r = cur.fetchall()
    for i in r:
        print(i)


if __name__ == "__main__":
    main()
