# -------------------------------------------------------------------------------
# Name: main.py
# Purpose: Pull data from Reddit
#
# Author(s):    David Little
#
# Created:      02/24/2021
# Updated:
# Update Comment(s):
#
# TO DO:
#
# -------------------------------------------------------------------------------

import pandas as pd
from pandas_datareader import data
import requests
import praw
import time

reddit = praw.Reddit(client_id="qa6M3klqKsXh8A",
                    client_secret="7gafRr64o5JN0LDX4Bg_9-zvn0yuHg",
                    redirect_uri="http://localhost:8080",
                    user_agent="asudatascience")
print(reddit.auth.url(["identity"], "...", "permanent"))


def submissions_pushshift_praw(subreddit, start=None, end=None, limit=1000, extra_query=""):
    """
    A simple function that returns a list of PRAW submission objects during a particular period from a defined sub.
    This function serves as a replacement for the now deprecated PRAW `submissions()` method.

    :param subreddit: A subreddit name to fetch submissions from.
    :param start: A Unix time integer. Posts fetched will be AFTER this time. (default: None)
    :param end: A Unix time integer. Posts fetched will be BEFORE this time. (default: None)
    :param limit: There needs to be a defined limit of results (default: 100), or Pushshift will return only 25.
    :param extra_query: A query string is optional. If an extra_query string is not supplied,
                        the function will just grab everything from the defined time period. (default: empty string)

    Submissions are yielded newest first.

    For more information on PRAW, see: https://github.com/praw-dev/praw
    For more information on Pushshift, see: https://github.com/pushshift/api
    """
    matching_praw_submissions = []

    # Default time values if none are defined (credit to u/bboe's PRAW `submissions()` for this section)
    utc_offset = 28800
    now = int(time.time())
    start = max(int(start) + utc_offset if start else 0, 0)
    end = min(int(end) if end else now, now) + utc_offset

    # Format our search link properly.
    search_link = ('https://api.pushshift.io/reddit/submission/search/'
                   '?subreddit={}&after={}&before={}&sort_type=created_utc&sort=asc&limit={}&q={}')
    search_link = search_link.format(subreddit, start, end, limit, extra_query)

    # Get the data from Pushshift as JSON.
    retrieved_data = requests.get(search_link)
    returned_submissions = retrieved_data.json()['data']

    # Iterate over the returned submissions to convert them to PRAW submission objects.
    for submission in returned_submissions:
        # Take the ID, fetch the PRAW submission object, and append to our list
        praw_submission = reddit.submission(id=submission['id'])
        matching_praw_submissions.append(praw_submission)

    # Return all PRAW submissions that were obtained.
    return matching_praw_submissions


start_date = 1609484400  # '2021-01-01 00:00:00' in Unix time
end_date = 1614581999  # '2021-02-28 23:59:59' in Unix time

posts = pd.DataFrame(columns=['title', 'flair', 'score', 'upvote_ratio', 'id',
                              'subreddit', 'url', 'num_comments', 'body', 'created'])  # Dataframe to store results

while start_date < end_date:  # Continue loop until end date is reached
    S = submissions_pushshift_praw(subreddit='wallstreetbets',
                                   start=start_date, end=end_date)  # Pull posts within date range
    for post in S:  # Looping through each post
        try:  # Try/except to catch any erroneous post pulls
            if post.selftext != '[removed]' and post.selftext != '[deleted]':  # Remove the deleted posts

                    posts = posts.append(
                        {'title': post.title,
                         'flair': post.link_flair_css_class,
                         'score': post.score,
                         'upvote_ratio': post.upvote_ratio,
                         'id': post.id,
                         'subreddit': post.subreddit,
                         'url': post.url,
                         'num_comments': post.num_comments,
                         'body': post.selftext,
                         'created': post.created}, ignore_index=True)  # Retrieve post data and append to dataframe
        except:
            next()  # Continue loop if error is found

    if len(S) < 100:  # To identify when the last pull is reached
        break
    start_date = posts['created'].max()  # Select the next earliest date to pull posts from
    print(start_date)  # An indicator of progress


posts['created'] = pd.to_datetime(posts['created'],unit='s')  # Converting Unix time to human readable
posts.to_csv('reddit_data.csv', index=False)  # Export data to .csv file


# -------------- FLAIR -------------------------------------------
import flair
flair_sentiment = flair.models.TextClassifier.load('en-sentiment')  # Load model

for index, row in posts.iterrows():  # Iterate over the rows of the dataframe
    s = flair.data.Sentence(row[0])  # Retrieve title of post
    flair_sentiment.predict(s)  # Predict sentiment
    posts['sentiment'][index] = s.labels[0]  # Add sentiment to dataframe
posts.to_csv('reddit_data_sentiment.csv') # Export results


# -------------- Stock Prices -------------------------------

start = pd.to_datetime('2021-01-01')
end = pd.to_datetime('2021-02-28')

gme = data.DataReader('GME', 'yahoo', start, end)
gme.to_csv('GME_Stocks.csv')

