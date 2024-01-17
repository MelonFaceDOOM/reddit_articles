import psycopg2
from db_client import PsqlClient
from config import NETWORK_DB_CREDENTIALS


conn = psycopg2.connect(**NETWORK_DB_CREDENTIALS)
client = PsqlClient(conn=conn)
client.make_core_tables()
