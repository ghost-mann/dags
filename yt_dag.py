from airflow import DAG
import os
import psycopg2
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from googleapiclient.discovery import build
from datetime import datetime, timedelta

# loading .env file
load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")

AIVEN_DB_CONFIG = {
    'host':os.getenv('AIVEN_DB_HOST'),
    'database_name':os.getenv('AIVEN_DB_NAME'),
    'user':os.getenv('AIVEN_USER'),
    'password':os.getenv('AIVEN_PASSWORD'),
    'port':os.getenv('AIVEN_PORT')
}

default_args = {
    'owner' : 'Austin',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=2)
}

def fetch_playlist_videos():
# yt api connection
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    playlist_id = ''
    
    # stores video IDs from playlist 
    videos = []
    
    # used to get subsequent page results
    # none means starts with first page
    next_page_token = None
    
    while True:
        response = youtube.playlistItems().list(
            # snippet contains basic information eg video ID
            part='snippet',
            maxResults=50,
            playlistId = playlist_id,
            pageToken=next_page_token
            
        # returns response as python dictionary
        ).execute()
        
        for item in response['items']:
            # extracts video ID from the item(nested)
            video_id = item["snippet"]["resourceId"]["videoId"]
            videos.append(video_id)
        
        # checks if there's a next page token   
        next_page_token = response.get("NextPageToken")
        if not next_page_token:
            break
        
        return videos

def get_video_stats_and_store():
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    # retrieves video IDs from specified playlist
    video_ids = fetch_playlist_videos()
    
    # stores video statistics
    all_stats = []
    # iterates through video IDs
    for i in range(0, len(video_ids), 50):
        # creates a sequence from 0 to 50 
        batch = video_ids[i:i + 50]
        # api call to retrieve data
        response = youtube.videos().list(
            part="statistics, snippet",
            id=",".join(batch)
        ).execute()
    
    # extracts and stores video info in dictionary
    for video in response["items"]:
        stats = {
            "video_id":video["id"],
            "title":video["snippet"]["title"],
            "views":int(video["statistics"].get("viewCount", 0)),
            "likes":int(video["statistics"].get("likeCount", 0))
        }
        all_stats.append(stats)
        
        # sort by views- desc
        all_stats.sort(key=lambda x: x["views"], reverse=True)
    
    
    # store in aiven postgresql
    # establishes connection to postgre
    # dictionary unpacked into keyword arguments for connect
    conn = psycopg2.connect(**AIVEN_DB_CONFIG)
    
    # used to execute sql queries against the db
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS yt_videos (
            video_id TEXT PRIMARY KEY,
            title TEXT,
            views INTEGER,
            likes INTEGER
        );
    """)
    
    for stat in all_stats:
        cur.execute("""
            INSERT INTO yt_videos (video_id, title, views, likes)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (video_id) DO UPDATE
            SET views = EXCLUDED.views,
                likes = EXCLUDED.likes;
        """, (stat["video_id"], stat["title"], stat["views"], stat["likes"]))
    conn.commit()
    cur.close()
    conn.close()
    
with DAG(
    dag_id="youtube_playlist_ranking",
    default_args=default_args,
    start_date=datetime(2024,6,1),
    schedule='@daily',
    catchup=False
) as dag:
    
    salutations = BashOperator(
        task_id = "greet_user",
        bash_command="echo 'Hello fellow traveller!'"
    )
    get_stats = PythonOperator(
        task_id = "get_video_stats_and_store",
        python_callable=get_video_stats_and_store
    )
    
    