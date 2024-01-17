from utilities import extract_domain


with open('blacklist.txt', 'r') as f:
    domain_blacklist = f.read().split('\n')


def main():
    with open('raw_url_list.txt', 'r', encoding='utf-8') as f:
        url_list = f.read().split('\n')
    url_list = filter_url_list(url_list)

    url_list = "\n".join(url_list)
    with open('filtered_url_list.txt', 'w', encoding='utf-8') as f:
        f.write(url_list)


def filter_url_list(url_list):
    url_list = remove_urls_from_blacklist(url_list)
    url_list = remove_urls_with_extensions(url_list)
    return url_list


def remove_urls_with_extensions(url_list):
    urls_without_extensions = []
    for url in url_list:
        if not url_has_extension(url):
            urls_without_extensions.append(url)
    return urls_without_extensions


def remove_urls_from_blacklist(url_list):
    urls_not_in_blacklist = []
    for url in url_list:
        if extract_domain(url) not in domain_blacklist:
            urls_not_in_blacklist.append(url)
    return urls_not_in_blacklist


def get_domain_counts(df):
    df['domain'] = df.apply(lambda x: extract_domain(x['url']), axis=1)
    df = df[~df['domain'].isin(domain_blacklist)]
    df['domain'].value_counts().reset_index().to_csv('top_10M_domains.csv')


def save_url_to_scrape_list(df):
    df['domain'] = df.apply(lambda x: extract_domain(x['url']), axis=1)
    df = df[~df['domain'].isin(domain_blacklist)]
    urls_to_scrape = df['url'].to_list()
    urls_to_scrape = "\n".join(urls_to_scrape)
    with open('urls_to_scrape.txt', 'w', encoding='utf-8') as f:
        f.write(urls_to_scrape)


def url_has_extension(url):
    """Looks in last 6 characters for a period, which implies an extension.
    Returns True if period found, and False if not."""
    last6 = url[-6:]
    if "." in last6:
        return True
    else:
        return False


if __name__ == "__main__":
    main()
