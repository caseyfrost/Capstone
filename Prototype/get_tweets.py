import tweepy
import pandas as pd
import psycopg2
from config import consumer_key, consumer_secret, access_token, access_token_secret
from datetime import datetime, timedelta
from sqlalchemy import create_engine


def get_tweets(query, amount):
    tweet_list = list(tweepy.Cursor(api.search_tweets, q=query, count=100, lang='en').items(amount))
    tweets = [[tweet.created_at, tweet.text, tweet.user._json['screen_name']] for tweet in tweet_list]
    return tweets


def df_to_db(conn_string, table, dataframe):
    """Appends the given dataframe to the given database table"""
    db = create_engine(conn_string)
    conn = db.connect()

    dataframe.to_sql(table, con=conn, if_exists='append', index=False)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    conn.commit()
    conn.close()


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today = datetime.today().strftime('%Y-%m-%d')

query = f'@SFBART OR #SFBART OR BART Delay since:{yesterday} until:{today}'

tweets = get_tweets(query=query, amount=300)

df = pd.DataFrame(tweets, columns=['created_date', 'text', 'handle', 'urls'])

print(df.to_string())
