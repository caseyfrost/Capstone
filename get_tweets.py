import tweepy
from tweepy import OAuthHandler
import pandas as pd

"""I like to have my python script print a message at the beginning. This helps me confirm whether everything is set 
up correctly. And it's nice to get an uplifting message ;). """

print("You got this!")

access_token = '210205203-cGtRZgBuDwicEwKAVxicFtawr0WOBVCKBEY6mw2G'
access_token_secret = '1dKrFmsxwxLI23SFfiEnlU6ZiyA9fxM7muYigP8KGMzft'
consumer_key = 'gSC4fVWgNpmxEpMDi2cr8Aeqm'
consumer_secret = 'T0mQlnu3yeD4FRKtMfm0Nvw1YnJR2Ra1oUAZeZYpxrBK5JK60L'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

tweets = []

count = 1

"""Twitter will automatically sample the last 7 days of data. Depending on how many total tweets there are with the 
specific hashtag, keyword, handle, or key phrase that you are looking for, you can set the date back further by 
adding since= as one of the parameters. You can also manually add in the number of tweets you want to get back in the 
items() section. """

for tweet in list(tweepy.Cursor(api.search_tweets, q="@SFBART", count=450).items(10)):

    print(count)
    count += 1

    try:
        data = [tweet.created_at, tweet.id, tweet.text, tweet.user._json['screen_name'], tweet.user._json['name'],
                tweet.user._json['created_at'], tweet.entities['urls']]
        data = tuple(data)
        tweets.append(data)

    except tweepy.TweepyException as e:
        print(e)
        continue

    except StopIteration:
        break

df = pd.DataFrame(tweets, columns=['created_at', 'tweet_id', 'tweet_text', 'screen_name', 'name',
                                   'account_creation_date', 'urls'])

print(df.head().to_string())

"""Add the path to the folder you want to save the CSV file in as well as what you want the CSV file to be named 
inside the single quotations """
# df.to_csv(path_or_buf='/Users/caseyfrost/Desktop/Springboard/GTFS_Capstone_Project/json_files/bart/tweets.nosync/test.csv',
          # index=False)
