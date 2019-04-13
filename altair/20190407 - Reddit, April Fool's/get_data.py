# took 20min to run

import requests
import pandas as pd
import praw
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.expand_frame_repr', False)

# https://praw.readthedocs.io/en/latest/getting_started/configuration/prawini.html#praw-ini
#reddit = praw.Reddit('bot1')
reddit = praw.Reddit(
    client_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    client_secret='yyyyyyyyyyyyyyyyyyyyyyyyyy',
    user_agent='zzzzzzzzzzzzzzzzzzzzzzzzzzzzz',
)

def main():    
    threads_page = requests.get('https://pastebin.com/raw/djbR067n')
    threads = threads_page.text.split('\r\n')
    
    df_rows = []
    for i, thread in enumerate(threads):
        print(f"processing #{i}: {thread}")

        submission = reddit.submission(url=thread)
        submission.comments.replace_more(limit=None)
        
        link_karma, comment_karma = get_karmas(submission.author)
        df_rows.append([
            datetime.utcfromtimestamp(submission.created_utc),
            datetime.utcfromtimestamp(submission.created_utc),
            True,
            submission.score,
            True if submission.url else False,
            submission.author,
            link_karma,
            comment_karma,
            True,
        ])
        
        for comment in submission.comments.list():
            link_karma, comment_karma = get_karmas(comment.author)
            
            df_rows.append([
                datetime.utcfromtimestamp(submission.created_utc),
                datetime.utcfromtimestamp(comment.created_utc),
                False,
                comment.score,
                False,
                comment.author,
                link_karma,
                comment_karma,
                submission.author == comment.author,
            ])

    df = pd.DataFrame(df_rows, columns=[
        'post_time',
        'comment_time',
        'is_original_post',
        'score', # upvotes - downvotes
        'url',
        'author',
        'post_karma',
        'comment_karma',
        'is_self_comment',
    ])
    
    df.to_csv('df.csv', index=False)

# catch deleted (author=None) and shadow-banned (userpage is 404)
def get_karmas(author):
    try:
        link_karma = author.link_karma
        comment_karma = author.comment_karma
    except Exception:
        link_karma = 0
        comment_karma = 0
    
    return link_karma, comment_karma
    
if __name__== "__main__":
    main()
