import tweepy
import pandas as pd
from config import consumer_key, consumer_secret, access_token, access_token_secret
from datetime import datetime, timedelta


def get_tweets(query, amount):
    tweet_list = list(tweepy.Cursor(api.search_tweets, q=query, count=100, lang='en').items(amount))
    tweets = [[tweet.created_at, tweet.text, tweet.user._json['screen_name'], tweet.entities['urls']] for tweet in tweet_list]
    return tweets


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today = datetime.today().strftime('%Y-%m-%d')

# query_1 = f'@SFBART Delay OR SFBARTalert OR #SFBART Delay filter:safe since:{yesterday} until:{today}'
# query_2 = '@SFBART Delay OR SFBARTalert OR #SFBART Delay filter:safe'
query = f'@SFBART OR #SFBART OR BART Delay since:{yesterday} until:{today}'

tweets = get_tweets(query=query, amount=300)

df = pd.DataFrame(tweets, columns=['created_at', 'tweet_text', 'screen_name', 'urls'])

print(df.to_string())
