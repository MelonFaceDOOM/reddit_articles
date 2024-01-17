"""parses reddit comments and saves relevant data while only read a chunk of the file at a time."""
import json
import os
import re
import csv

with open('../raw_processing/thread_ids.txt', 'r') as f:
    thread_ids = f.read().strip().split('\n')
    

data_keys = ["all_awardings", "author", "author_created_utc", "author_fullname", "body", "controversiality", "created_utc", "gilded", "gildings", "id", "is_submitter", "link_id", "parent_id", "permalink", "score", "stickied", "subreddit", "subreddit_id", "subreddit_type", "top_awarded_type", "total_awards_received"]
compressed_folder = "D:/data/reddit_data/comments/compressed"    
decompressed_folder = "D:/data/reddit_data/comments/decompressed"  
parsed_folder = "D:/data/reddit_data/comments/parsed"


def main():
    test_link_id()
            
            
def extract_data_from_parsed_file(parsed_file, data_keys):
    with open(parsed_file, 'r') as f:
        parsed_data = json.load(f)
    output_data = []
    for d in parsed_data:
        extracted_data = {}
        for data_key in data_keys:
            extracted_data[data_key] = extract_key_from_parsed_data(d, data_key)
        if extracted_data['link_id'] in thread_ids:            
            output_data.append(extracted_data)
    return output_data
    
def test_link_id(parsed_file, data_keys):
 # RESULTS:
 # 0 matches to thread list found!
 # 1391055 instances where permalink contained link_id
 # 1619 instances where link_id wasn't in permalink
 # 0 instances with no link_id
 # 0 instances with no permalink
 # The 1619 were all just regex issues with subreddits have "comments" in the name.
 # link_id is always thread_id (with 3 extra chars at the beginning)
 # 0 matches to thread list isn't a concern, since i only ran this on a portion of the oldest
 # comments, so I think the news articles might just not go back that far
    with open(parsed_file, 'r') as f:
        parsed_data = json.load(f)
    output_data = []
    permalink_is_link_id = 0
    permalink_not_link_id = []
    no_link_id = []
    no_permalink = []
    pattern = ".*?comments\/([\w\d]+).*"
    for d in parsed_data:
        extracted_data = {}
        for data_key in data_keys:
            extracted_data[data_key] = extract_key_from_parsed_data(d, data_key)
        link_id = None
        permalink = None
        try:
            link_id = extracted_data['link_id'][3:]
        except:
            no_link_id.append(str(extracted_data))
        try:
            permalink = extracted_data['permalink'][3:]
        except:
            no_permalink.append(str(extracted_data))
        if link_id and permalink:
            match = re.match(pattern, permalink)
            permalink_thread_id = match.group(1)
            if permalink_thread_id == link_id:
                permalink_is_link_id += 1
            else:
                permalink_not_link_id.append(f"{permalink}, {link_id}")
            if link_id in thread_ids:            
                output_data.append(extracted_data)
            
    print(len(output_data), "matches to thread list found!")
    print(permalink_is_link_id, "instances where permalink contained link_id")
    print(len(permalink_not_link_id), "instances where link_id wasn't in permalink")
    print(len(no_link_id), "instances with no link_id")
    print(len(no_permalink), "instances with no permalink")
    
    with open('0_permalink_not_link_id.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(permalink_not_link_id))
    with open('0_no_link_id.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(no_link_id))
    with open('0_no_permalink.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(no_permalink))
    
def extract_key_from_parsed_data(parsed_data, data_key):
    if data_key in parsed_data:
        extracted_data = parsed_data[data_key]
    else:
        extracted_data = ''
    if not extracted_data:
        extracted_data = ''
    return extracted_data


def save_data_to_existing_csv(csv_file, data, headers):
    """will create the csv if it doesn't already exist"""
    file_already_exists = os.path.isfile(csv_file)
    with open(csv_file, 'a+') as f:
        writer = csv.writer(f, escapechar='\\')
        if not file_already_exists:
            writer.writerow(headers)
        for row in data:
            writer.writerow([row[header] for header in headers])


if __name__ == "__main__":
    main()

