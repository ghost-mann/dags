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
    'host': os.getenv('AIVEN_DB_HOST'),
    'database': os.getenv('AIVEN_DB_NAME'),  # Fixed: should be 'database' not 'database_name'
    'user': os.getenv('AIVEN_USER'),
    'password': os.getenv('AIVEN_PASSWORD'),
    'port': os.getenv('AIVEN_PORT')
}

default_args = {
    'owner': 'Austin',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def fetch_playlist_videos():
    """Fetch all video IDs from a YouTube playlist"""
    try:
        # yt api connection
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        playlist_id = 'PLDbSvEZka6GGanXjSfH1bQNVheppFQWWo'
        
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
                playlistId=playlist_id,
                pageToken=next_page_token
            # returns response as python dictionary
            ).execute()
            
            for item in response['items']:
                # extracts video ID from the item(nested)
                video_id = item["snippet"]["resourceId"]["videoId"]
                videos.append(video_id)
            
            # checks if there's a next page token (Fixed: correct case)
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
        
        # Fixed: return statement outside the loop
        print(f"Fetched {len(videos)} videos from playlist")
        return videos
        
    except Exception as e:
        print(f"Error fetching playlist videos: {str(e)}")
        raise

def get_video_stats_and_store():
    """Get video statistics and store them in PostgreSQL database"""
    try:
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        # retrieves video IDs from specified playlist
        video_ids = fetch_playlist_videos()
        
        if not video_ids:
            print("No videos found in playlist")
            return
        
        # stores video statistics (Fixed: moved outside loop)
        all_stats = []
        
        # iterates through video IDs in batches of 50
        for i in range(0, len(video_ids), 50):
            # creates a batch of up to 50 video IDs
            batch = video_ids[i:i + 50]
            print(f"Processing batch {i//50 + 1}: {len(batch)} videos")
            
            # api call to retrieve data (Fixed: removed space in part parameter)
            response = youtube.videos().list(
                part="statistics,snippet",
                id=",".join(batch)
            ).execute()
            
            # extracts and stores video info in dictionary
            for video in response["items"]:
                stats = {
                    "video_id": video["id"],
                    "title": video["snippet"]["title"],
                    "views": int(video["statistics"].get("viewCount", 0)),
                    "likes": int(video["statistics"].get("likeCount", 0))
                }
                all_stats.append(stats)
        
        # Fixed: sort after collecting all stats
        all_stats.sort(key=lambda x: x["views"], reverse=True)
        print(f"Collected stats for {len(all_stats)} videos")
        
        # store in aiven postgresql
        # establishes connection to postgre
        # dictionary unpacked into keyword arguments for connect
        conn = psycopg2.connect(**AIVEN_DB_CONFIG)
        
        # used to execute sql queries against the db
        cur = conn.cursor()
        
        # Create table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS yt_videos (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                views INTEGER,
                likes INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Insert/update video statistics
        for stat in all_stats:
            cur.execute("""
                INSERT INTO yt_videos (video_id, title, views, likes)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE
                SET views = EXCLUDED.views,
                    likes = EXCLUDED.likes,
                    updated_at = CURRENT_TIMESTAMP;
            """, (stat["video_id"], stat["title"], stat["views"], stat["likes"]))
        
        conn.commit()
        print(f"Successfully stored {len(all_stats)} video records")
        
    except Exception as e:
        print(f"Error in get_video_stats_and_store: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# DAG definition
with DAG(
    dag_id="youtube_playlist_ranking",
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule='@daily',
    catchup=False,
    description='Fetch YouTube playlist video stats and store in PostgreSQL'
) as dag:
    
    salutations = BashOperator(
        task_id="greet_user",
        bash_command="echo 'Hello fellow traveller!'"
    )
    
    get_stats = PythonOperator(
        task_id="get_video_stats_and_store",
        python_callable=get_video_stats_and_store
    )
    
    # Set task dependencies
    salutations >> get_stats