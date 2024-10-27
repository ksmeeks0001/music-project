from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import datetime
import time

from SpotifyAPI.spotify import Spotify, Search


def search_new_artists():
    
    mysql_hook = MySqlHook(mysql_conn_id='music-db')
    dbconn = mysql_hook.get_conn()
    cursor = dbconn.cursor()
    
    cursor.execute("select Id, Name from Artist where SpotifyId is null")
    artists = cursor.fetchall()
    
    spotify = Spotify(Variable.get("spotify_client_id"), Variable.get("spotify_client_secret"))
    for artist_id, artist_name in artists:
        resp = spotify.search(artist_name, Search.ARTIST, limit=10)
        
        for item in resp['artists']['items']:
            if item['name'] == artist_name:
                if len(item.get('images', [])) > 0:
                    image_url = item['images'][0]['url']
                else:
                    image_url = None
                    
                cursor.execute("update Artist set SpotifyId = %s, ImageURL = %s where Id = %s", (item['id'], image_url, artist_id))
                
                for genre in item['genres']:
                    cursor.callproc("update_genre", [artist_id, genre])
                    
                dbconn.commit()
                
                break
        time.sleep(1)       
                
def update_artists():
    
    mysql_hook = MySqlHook(mysql_conn_id='music-db')
    dbconn = mysql_hook.get_conn()
    cursor = dbconn.cursor()
    
    cursor.execute("select Id, SpotifyId from Artist where SpotifyId is not null")
    artists = cursor.fetchall()
    
    spotify = Spotify(Variable.get("spotify_client_id"), Variable.get("spotify_client_secret"))
    for artist_id, spotify_id in artists:
        
        resp = spotify.artists(spotify_id)
        cursor.execute(f"""
        update Artist 
        set 
            SpotifyFollowers = %s,
            SpotifyPopularity = %s,
            SpotifyUpdateTime = now()
        where id = %s
        """,
        (resp['followers']['total'], resp['popularity'],  artist_id)
        )
        time.sleep(1)
        dbconn.commit()
        
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1), 
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['ksmeeks0001@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spotify_pull',
    default_args=default_args,
    description='Pull artist data from spotify',
    schedule_interval='@daily',  
) as dag:

    search_new_artists_task = PythonOperator(
        task_id='search_new_artists',
        python_callable=search_new_artists
    )
    
    update_artists_task = PythonOperator(
        task_id='update_artists',
        python_callable=update_artists
    )

    search_new_artists_task >> update_artists_task
