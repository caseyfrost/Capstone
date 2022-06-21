import tweepy
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from config import consumer_key, consumer_secret, access_token, access_token_secret, db_con_str
from create_delay_events_dag import df_to_db
from datetime import datetime, timedelta


def get_tweets(api_object, query, amount):
    tweet_list = list(tweepy.Cursor(api_object.search_tweets, q=query, count=100, lang='en').items(amount))
    tweets = [[tweet.created_at, tweet.text, tweet.user._json['screen_name']] for tweet in tweet_list]
    return tweets


def main():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.today().strftime('%Y-%m-%d')

    # query to look for tweets about BART delays
    query = f'@SFBART OR #SFBART OR BART Delay since:{yesterday} until:{today}'

    tweets = get_tweets(api_object=api, query=query, amount=300)

    df = pd.DataFrame(tweets, columns=['created_date', 'text', 'handle'])

    df_to_db('GTFS', 'Tweets', df, db_con_str)


default_args = {
    'owner': 'cfrost',
    'start_date': '2022-05-30',
    'end_date': '2023-05-30',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    dag_id='bart_get_tweets',
    default_args=default_args,
    description='Collect tweets from the prior day that match the query about BART delays',
    schedule_interval='0 2 * * *',
    catchup=False
)

t0 = PythonOperator(
    task_id='bart_tweets',
    dag=dag,
    python_callable=main
)

t0
