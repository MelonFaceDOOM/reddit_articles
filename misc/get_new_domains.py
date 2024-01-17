import pandas as pd
import re 


def main():
    with open('new_domains.txt', 'r', encoding='utf-8') as f:
        domains = f.read().split('\n')
    print(len(domains))
    misinfo_labeled = pd.read_csv('misinfo_labeled.csv', names=['domain','misinfo'])
    new_domains = []
    for domain in domains:
        if domain not in misinfo_labeled['domain'].tolist():
            new_domains.append(domain)
    print(len(new_domains))
    new_domains_labeled = zip(new_domains,["unknown"]*len(new_domains))
    df2 = pd.DataFrame(new_domains_labeled, columns=['domain','misinfo'])
    df3 = pd.concat([misinfo_labeled, df2])
    df3.to_csv('new_domains_labeled.csv', index=False)
        

def save_new_domains():
    subs_df = pd.read_csv('subreddits_misinfo_data.csv')
    subs = subs_df[subs_df['misinfo_ratio']>0.3]['subreddit'].tolist()
    df = pd.read_csv('data_from_worst_subs.csv')
    df['domain'] = df.apply(lambda x: extract_domain(x['url']), axis=1)
    df = df[df['subreddit'].isin(subs)]
    df = df[['domain','score']]
    df = df.groupby('domain', as_index=False).sum()
    df = df.sort_values(by=['score'],ascending=False)
    with open('new_domains.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(df['domain'].tolist()))


def extract_domain(url):
    pattern = '(?:https?:\/\/)?(?:www\.)?((?:[\w-]+\.)+(?:\w+)+).*'
    try:
        match = re.match(pattern, url)
    except:
        match = ''
    if not match:
        return ''
    return match.group(1)
    

if __name__ == "__main__":
    main()
