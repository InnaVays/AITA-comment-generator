from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime as dt
import time

def get_date(created):
    return dt.datetime.fromtimestamp(created).strftime('%d-%m-%Y')

# set initial parameters
headers = {'User-agent': 'Apple-Air AITA scraper Weird Opportunity and some other stuff for unique user agent'}
url = 'https://api.pushshift.io/reddit/search/submission/'

data = []
data_empt = []

thread_name = 'AmItheAsshole'
params = {'subreddit': thread_name, 'size': 500}

latest_posts_num = 15000
# retrieve 15000 lateest post from pushshift api
while len(data) < latest_posts_num:
    response = requests.get(url, headers=headers, params=params)
    time.sleep(1)
    if response.status_code == 200:
        json_data = response.json()
        posts = json_data['data']
        for post in posts:
            # check for duplicates
            if post['id'] not in [d['id'] for d in data]:  
                post_data = {}
                post_data['title'] = post['title']
                # post text
                post_data['text'] = post['selftext']
                post_data['date'] = get_date(post['created_utc'])
                post_data['num_upvotes'] = post['score']
                post_data['num_comments'] = post['num_comments']
                post_data['id'] = post['id']
                
                # retrive data from OLD reddit to get the most popular comment
                if post_data['text'] != '[removed]':
                    comments_url = f"https://old.reddit.com/r/{thread_name}/comments/{post_data['id']}/"
                    response = requests.get(comments_url, headers=headers)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    entry_div = soup.find_all('div', class_="entry unvoted")
                    if len(entry_div) > 3:
                        # text score
                        post_upvotes = soup.find_all(class_="score unvoted")[0]
                        if post_upvotes != None:
                            post_data['num_upvotes'] = post_upvotes.get('title')
                        else:
                            post_data['num_upvotes'] = None
                        # first comment
                        first_comment = entry_div[2].find(action="#", class_="usertext warn-on-unload")
                        if first_comment != None:
                            post_data['first_comment'] =first_comment.text
                        else:
                            post_data['first_comment_upvotes'] = None
                        #first comment score
                        first_comment_upvotes = entry_div[2].find(class_="score unvoted")
                        if first_comment_upvotes != None:
                            post_data['first_comment_upvotes'] = first_comment_upvotes.get('title')
                        else:
                            post_data['first_comment_upvotes'] = None
                        data.append(post_data)
                        # save copy (connection crushes a lot)                        
                        if len(data) % 500 == 0:
                            df = pd.DataFrame(data)
                            df.to_csv(f'{thread_name}_latest_{len(data)}_posts.csv', index=False)
                            print(f"{len(data)} posts saved to CSV file.")
        
        # update time parameter for next iteration. UTC format
        params['before'] = posts[-1]['created_utc']
        # set time delay. Reddit cap is 60 request per minute
        time.sleep(1)
        
    else:
        print(f"Error {response.status_code}.")
        break

