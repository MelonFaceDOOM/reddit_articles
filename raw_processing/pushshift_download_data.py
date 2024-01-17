import datetime
import requests
from lxml import html
import os
import re
import time

"""this source is now deprecated by torrents"""


def main():
#    data_url = "https://files.pushshift.io/reddit/comments/"
    data_url = "https://files.pushshift.io/reddit/submissions/"
    save_folder = "/data/reddit_data/submissions/compressed/"
    start_date = datetime.date.fromisoformat("2021-01-01")
    comments_data_page = requests.get(data_url)
    tree = html.fromstring(comments_data_page.text)
    monthly_file_urls = tree.xpath("//tr[@class='file']/td[1]/a")
    monthly_file_urls = [element.get('href') for element in monthly_file_urls]
    monthly_file_urls = [m[2:] for m in monthly_file_urls]
    already_downloaded_files = get_file_names_from_folder(save_folder)
    monthly_file_urls = [m for m in monthly_file_urls if m not in already_downloaded_files]
    monthly_file_urls = [data_url+m for m in monthly_file_urls]
    
    urls_to_scrape = []
    for url in monthly_file_urls:
        date = get_date_from_url(url)
        if date and date >= start_date:
            urls_to_scrape.append(url)
            
    for url in urls_to_scrape:
        download_failed = True     
        while download_failed:
            try:
                download_file(url, save_folder=save_folder)
                print("download completed for url: ", url)
                download_failed = False
            except Exception as e:
                print(e)
                print("download failed for url: ", url)
                time.sleep(10)


def get_date_from_url(url):
    match = re.search(pattern="(\d{4}-\d{2})", string=url)
    date = None
    if match:
        date = match.group(1) + "-01"
        date = datetime.date.fromisoformat(date)
    return date 
        
    
def download_file(url, save_folder):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        save_location = save_folder + url.split('/')[-1]
        with open(save_location, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return save_location


def get_file_names_from_folder(folder_path):
    # also looks in subfolders
    file_names = []
    for subdir, dirs, files in os.walk(folder_path):
        file_names += files
    return file_names
    
    
if __name__ == "__main__":
    main()
