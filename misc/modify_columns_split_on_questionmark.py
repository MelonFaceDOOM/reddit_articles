from config import LOCAL_DB_CREDENTIALS
import psycopg2


# UPDATE: not going to do this since sometimes ? contains actual information that defines what the page is. Sometimes
# it contains a bunch of referrer bullshit or something, which should be removed, but I'd have to find a way to
# isolate just those. For now I'll leave it as is. There will be some matches that don't get made because of slightly
# different urls, but I believe this will be a relatively small % of rows (article urls contain ? 7.8k/210k. that's
# more than I want, but not large enough for it ot be the top priority right now)

def main():
    conn = psycopg2.connect(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute("""SELECT count(*) FROM article WHERE url_trimmed NOT LIKE '%?%' """)
    r = cur.fetchone()
    print(r)


def split_on_qm(url):
    url = url.split("?")[0]
    return url


if __name__ == "__main__":
    main()
