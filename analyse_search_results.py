import os
import pandas as pd
import matplotlib.pyplot as plt


csv_articles = 'search_results/search_test_6/articles.csv'
csv_submissions = 'search_results/search_test_6/submissions.csv'
csv_comments = 'search_results/search_test_6/comments.csv'
output_folder = 'search_results/search_test_6/analysis'


def main():
    save_subset_with_sub_search_terms(["lab_leak", "bioweapon"], 'unnatural_origin.csv')


def split_file_into_sub_search_terms():
    """sub_search_terms are in the "tags" column. If the term is in the article, it will be in the tags column"""
    articles = pd.read_csv(csv_articles)
    for i in range(30):
        print(articles.iloc[i]['article_title'])
    print(articles['tags'].value_counts())
    value_counts_series = articles['tags'].value_counts().head(8)
    for category, count in zip(value_counts_series.index, value_counts_series.values):
        df = articles[articles['tags'] == category]
        df.to_csv(f'search_results/search_test_6/articles_{category}.csv', index=False)


def save_subset_with_sub_search_terms(sub_search_terms, output_filename):
    """saves articles, submissions, and comments where a specific sub_search_term has been identified in the tags column"""
    df_articles = pd.read_csv(csv_articles)
    df_submission = pd.read_csv(csv_submissions)
    df_comments = pd.read_csv(csv_comments)
    articles_with_terms = df_articles[df_articles.apply(lambda x: terms_in_tags(sub_search_terms, x['tags']), axis=1)]
    url_hashes = articles_with_terms['url_hash'].tolist()
    submissions_with_terms = df_submission[df_submission['url_hash'].isin(url_hashes)]
    link_ids = submissions_with_terms['id'].to_list()
    comments_with_terms = df_comments[df_comments['link_id'].isin(link_ids)]
    articles_with_terms.to_csv(os.path.join(output_folder, f'articles_{output_filename}'), index=False)
    submissions_with_terms.to_csv(os.path.join(output_folder, f'submissions_{output_filename}'), index=False)
    comments_with_terms.to_csv(os.path.join(output_folder, f'comments_{output_filename}'), index=False)


def terms_in_tags(terms_to_search, tags_text):
    """as long as one term is found, return True"""
    tags = str(tags_text).split(',')
    for term in terms_to_search:
        if term in tags:
            return True
        else:
            return False


def plot_submission_upvotes_over_time(submissions_csv_file):
    df = pd.read_csv(submissions_csv_file)
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
    df['day'] = df['created_utc'].dt.date
    grouped_df = df.groupby('day')['score'].sum().reset_index()
    grouped_df['7_day_avg'] = grouped_df['score'].rolling(window=7).mean()
    plt.figure(figsize=(10, 6))
    plt.plot(grouped_df['day'], grouped_df['7_day_avg'], marker='o', label='7-day Moving Average')
    plt.title('7-day Average Score of Posts of Articles Mentioning "lab leak" or bioweapon')
    plt.xlabel('Day')
    plt.ylabel('Reddit score')
    plt.legend()
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    main()
