import psycopg2
from db_client import PsqlClient
from config import LOCAL_DB_CREDENTIALS


"""The purpose of this was to test having doing joins on articles when they 
 are stored in a separate database, but it seemed slower so i didn't proceed with this"""


# def create_fdw(self):
#     cur = self.conn.cursor()
#     cur.execute('''CREATE EXTENSION IF NOT EXISTS postgres_fdw;''')
#     cur.close()

# def create_news_link(self, local_user, local_password, remote_server, remote_host, remote_port, remote_user, remote_password):
#     cur = self.conn.cursor()
#     cur.execute(f"""CREATE SERVER {remote_server}
#                    FOREIGN DATA WRAPPER postgres_fdw
#                    OPTIONS (host '{remote_host}', port '{remote_port}', dbname '{remote_server}');""")
#     cur.execute(f"""CREATE USER MAPPING FOR {local_user}
#                    SERVER {remote_server}
#                    OPTIONS (user '{remote_user}', password '{remote_password}');""")
#     cur.execute(f"""IMPORT FOREIGN SCHEMA public
#                    FROM SERVER {remote_server}
#                    INTO public;""")
#     self.conn.commit()
#     cur.close()


def main():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    # remote and local credentials are the same in this case
    client.create_news_link(local_user=LOCAL_DB_CREDENTIALS['user'], local_password=LOCAL_DB_CREDENTIALS['password'],
                            remote_server='news', remote_host=LOCAL_DB_CREDENTIALS['host'],
                            remote_port=LOCAL_DB_CREDENTIALS['port'], remote_user=LOCAL_DB_CREDENTIALS['user'],
                            remote_password=LOCAL_DB_CREDENTIALS['password'])
    # test success by selecting from a remote table
    cur = conn.cursor()
    cur.execute('''SELECT * from article limit 10''')
    r = cur.fetchall()
    print(r)
    cur.close()


if __name__ == "__main__":
    main()
