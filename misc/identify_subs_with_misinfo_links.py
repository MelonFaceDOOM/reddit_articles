import os
import re
import csv
import pandas as pd


"""This was used when I didn't think I'd import all the data into a db,
   so I manually indentified subs where content related to covid was likely to be spread,
   I extracted data only from those, and then I found the subs that posted the highest % of 
   misinfo links (according to factcheckers)."""


misinfo_file = 'misinfo.txt'
with open(misinfo_file, 'r') as f:
    misinfo = f.read().strip().split('\n')
    
    
def main():
#    save_top_domains_from_worst_subs()
#    extract_subreddit_list_from_misinfo_links
    pass


def extract_subreddit_list_from_misinfo_links():
    """deprecated: uses raw files that don't exist any more because now,
       all posts are saved to db, so i don't need to analyse csv extracts
       opens all_links_from_worst_subs.csv (produced by save_all_links_from_sub_with_high_misinfo_count()), and
       filters the data to only subreddits from a specified list"""
    with open('worst_subs.txt', 'r') as f:
        subs = f.read()
    subs = subs.strip().split('\n')
    df = pd.read_csv('all_links_from_worst_subs_2.csv', encoding='utf-8')
    df = df[df['subreddit'].isin(subs)]
    with open('data_from_worst_subs.csv', 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(list(df.columns))
        writer.writerows(df.values.tolist())


def save_all_links_from_sub_with_high_misinfo_count():
    """deprecated: uses raw files that don't exist any more because now,
       all posts are saved to db, so i don't need to analyse csv extracts
       finds top misinfo subreddits
       saves all links from those misinfo subreddits"""
    df_misinfo = merge_misinfo_csv_files_into_df()
    top_misinfo_subreddits = get_top_misinfo_subreddits(df_misinfo, n=100)
    df_misinfo_from_all_links = filter_all_links_to_misinfo(top_misinfo_subreddits)
    with open('all_links_from_worst_subs.csv', 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(list(df_misinfo_from_all_links.columns))
        writer.writerows(df_misinfo_from_all_links.values.tolist())


def merge_misinfo_csv_files_into_df():
    misinfo_files = get_file_paths_from_folder('../results')
    misinfo_files = [f for f in misinfo_files if 'misinfo' in f]  # filter out other files in folder
    df = create_df_from_files(misinfo_files)
    return df


def get_top_misinfo_subreddits(df, n):
    df = df[['subreddit','score']]
    df = df.groupby(['subreddit']).sum().sort_values(by=['score'], ascending=False)
    print(df.head(30))
    top_subreddit_list = list(df.index[:n])
    return(top_subreddit_list)


def filter_all_links_to_misinfo(top_misinfo_subreddits):
    all_links_files = get_file_paths_from_folder('../results')
    all_links_files = [f for f in all_links_files if 'misinfo' not in f]
    filtered_dfs = []
    chunksize = 10 ** 7
    for f in all_links_files:
        with pd.read_csv(f, chunksize=chunksize, low_memory=False) as reader:
            for chunk in reader:
                chunk = chunk[chunk['subreddit'].isin(top_misinfo_subreddits)]
                filtered_dfs.append(chunk)
    df = pd.concat(filtered_dfs)
    return df


def save_top_domains_from_worst_subs():
    df = pd.read_csv('all_links_from_worst_subs_2.csv')
    df['domain'] = df.apply(lambda x: extract_domain(x['url']), axis=1)  
    df['misinfo'] = df.apply(lambda x: x['domain'] in misinfo, axis=1)
    df = df[['subreddit', 'misinfo', 'score']]
    df = df.groupby(['subreddit', 'misinfo'], as_index=False).sum()
    subreddits = df['subreddit'].unique().tolist()
    misinfo_data = []
    for subreddit in subreddits:
        misinfo_score = df[(df['subreddit']==subreddit) & (df['misinfo']==True)]['score'].iloc[0]
        try:
            other_score = df[(df['subreddit']==subreddit) & (df['misinfo']==False)]['score'].iloc[0]
        except:
            other_score = 0
        total_score = misinfo_score + other_score
        misinfo_ratio = misinfo_score / total_score
        subreddit_misinfo_data = [subreddit, misinfo_score, total_score, misinfo_ratio]
        misinfo_data.append(subreddit_misinfo_data)
        
    df2 = pd.DataFrame(misinfo_data, columns=['subreddit', 'misinfo_score', 'total_score', 'misinfo_ratio'])
    df2 = df2.sort_values(by=['misinfo_ratio'], ascending=False)
    df2.to_csv('subreddits_misinfo_data.csv')
#    df.head(500).to_csv('top_500_domains_2.csv', encoding='utf-8')


def create_df_from_files(files):
    dfs = [pd.read_csv(f) for f in files]
    df = pd.concat(dfs)
    return df
    

def get_file_paths_from_folder(folder_path):
    file_paths = []
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(folder_path, file)
            file_paths.append(file_path)
    return file_paths
            

def extract_domain(url):
    if pd.isna(url):
        return url
    pattern = '(?:https?:\/\/)?(?:www\.)?((?:[\w-]+\.)+(?:\w+)+).*'
    match = re.match(pattern, url)
    if not match:
        return ''
    return match.group(1)
    

if __name__ == "__main__":
    main()

