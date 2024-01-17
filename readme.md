A library designed to import pushshift dumps to a postgresql db, scrape articles in submissions, and pull data from the db.

1) install postgres, create a user, give it superuser privs
2) create database in psql (i.e. "CREATE DATABASE REDDIT")
3) create config.py in project directory and put a dict like this in it:

~~~
NETWORK_DB_CREDENTIALS = {
    'user': [username],
    'password': [password],
    'host': [ip address or 'localhost'],
    'port': [port],
    'database': [db_name]
}
~~~
4) set up the db tables by running create_db_tables.py
5) download and save monthly comment/submission pushshift files
6) import them to database using import_raw_data_to_db.py (note this can take an hour or more per file)
