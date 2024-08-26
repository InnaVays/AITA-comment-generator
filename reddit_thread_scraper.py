import pandas as pd
import csv
import datetime
import time
import praw

client_id = 'YOUR_CLIENT_ID'

client_secret = 'YOUR_REDDIT_SECRET'

user_agent='my_AITA_writer bot v1.0 by OpportunityWeird8029'

reddit = praw.Reddit(client_id=client_id,
                     client_secret=client_secret,
                     user_agent=user_agent)


def fetch_all_posts(subreddit_name, total_posts, reddit=reddit):

    # Define the subreddit
    subreddit = reddit.subreddit(subreddit_name)

    # Variables for tracking progress
    posts = []
    after = None  # To store the 'after' token for pagination

    # Loop to fetch posts until we reach the desired number or no more posts are available
    while len(posts) < total_posts:
        # Fetch the posts using the 'after' token to get the next batch of posts
        new_posts = list(subreddit.new(limit=1000, params={'after': after}))
        
        if not new_posts:
            print("No more posts available.")
            break
        
        posts.extend(new_posts)
        after = new_posts[-1].fullname  # Get the fullname of the last post in the batch

        print(f"Fetched {len(posts)} posts so far...")

        # Respect Reddit's API rate limit
        time.sleep(2)  # Adjust the sleep time as necessary

        if len(new_posts) < 1000:
            # Less than 1000 posts in this batch, likely means we're at the end
            print("Reached the end of available posts.")
            break

    # Truncate the list to the exact number of desired posts
    posts = posts[:total_posts]
    print(f"Total posts retrieved: {len(posts)}")
    
    return posts

def build_dataframe_from_posts(posts):
    # List to store the post data
    data = []

    for post in posts:
        # Convert the post's creation timestamp to a human-readable date
        post_date = datetime.datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S')

        post_info = {
            "title": post.title,
            "score": post.score,
            "date": post_date,
            "text": post.selftext
        }

        # Fetch the comment with the most upvotes
        post.comments.replace_more(limit=0)
        top_comment = None
        max_upvotes = 0

        for comment in post.comments:
            if comment.score > max_upvotes:
                top_comment = comment.body
                max_upvotes = comment.score
                comment_date = datetime.datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S')

        post_info["comments_num"] = len(post.comments)
        post_info["top_comment"] = top_comment
        post_info["top_comment_upvotes"] = max_upvotes
        post_info["comment_date"] = comment_date if top_comment else None

        data.append(post_info)

    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(data)
    return df

def scrape_reddit_posts(subreddit_name, total_posts, reddit=reddit, resume_from_batch=1):

    # Define the subreddit
    subreddit = reddit.subreddit(subreddit_name)

    # Variables for tracking progress
    post_count = 0
    batch_number = resume_from_batch
    query_limit = 100  # 100 queries per minute for OAuth clients

    # List to store the data
    data = []
    print(len(subreddit.new(limit=total_posts)))
    # Fetch the posts with rate limiting
    for post in subreddit.new(limit=total_posts):
    
        if post_count < (batch_number - 1) * 500:
            post_count += 1
            continue  # Skip posts already processed

        # Convert the post's creation timestamp to a human-readable date
        post_date = datetime.datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S')

        post_info = {
            "title": post.title,
            "url": post.url,
            "score": post.score,
            "id": post.id,
            "created_utc": post.created_utc,
            "date": post_date
        }

        # Fetch the comment with the most upvotes
        post.comments.replace_more(limit=0)
        top_comment = None
        max_upvotes = 0

        for comment in post.comments:
            if comment.score > max_upvotes:
                top_comment = comment.body
                max_upvotes = comment.score
                comment_date = datetime.datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S')

        post_info["comments_num"] = len(post.comments)
        post_info["top_comment"] = top_comment
        post_info["top_comment_upvotes"] = max_upvotes
        post_info["comment_date"] = comment_date if top_comment else None

        data.append(post_info)
        post_count += 1

        # Check if we need to save the current batch
        if post_count % 500 == 0 or post_count == total_posts:
            # Define the CSV file name for this batch
            csv_file = f"reddit_posts_batch_{batch_number}.csv"

            # Write the data to a CSV file
            with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=["title", "url", "score", "id", "created_utc", "date", "comments_num","top_comment", "top_comment_upvotes", "comment_date"])
                writer.writeheader()
                for entry in data:
                    writer.writerow(entry)

            print(f"Batch {batch_number} with {len(data)} posts has been written to {csv_file}")

            # Clear the data list for the next batch and increment batch number
            data.clear()
            batch_number += 1

            # Sleep to respect rate limits
            print(f"Processed {post_count} posts. Sleeping for 60 seconds to respect API rate limits.")
            time.sleep(60)  # Increase sleep time to avoid rate limit issues

        
    else:
        print(f"Error {response.status_code}.")
        break

