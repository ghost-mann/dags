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


    