# etl/youtube_s3_mongo.py

import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
import pandas as pd
import boto3
from io import StringIO
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import isodate

# ---- Step 1: Load environment variables ----
dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path)

# ---- Step 2: Fetch sensitive credentials from .env ----
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")


# ---- Step 3: Define ETL Functions ----
def convert_iso8601_duration(duration):
    """Convert ISO 8601 duration to seconds."""
    if not duration:
        return None
    return int(isodate.parse_duration(duration).total_seconds())


# - Step 4: Extract YouTube Data ----
def extract_youtube_data(keywords, max_pages=10):
    """Extract YouTube videos for multiple keywords within the last two years, with pagination support."""
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    result = {}

    # Date two years ago
    two_years_ago = datetime.now(timezone.utc) - timedelta(days=730)

    for keyword in keywords:
        videos = []
        next_page_token = None
        page_count = 0

        while page_count < max_pages:
            search_response = (
                youtube.search()
                .list(
                    part="snippet",
                    q=keyword,
                    type="video",
                    maxResults=50,  # Max allowed per request
                    order="viewCount",
                    publishedAfter=two_years_ago.isoformat(timespec="seconds").replace(
                        "+00:00", "Z"
                    ),
                    pageToken=next_page_token,
                )
                .execute()
            )

            video_ids = [
                item["id"]["videoId"] for item in search_response.get("items", [])
            ]
            if not video_ids:
                break

            video_response = (
                youtube.videos()
                .list(
                    part="snippet,statistics,contentDetails,status",
                    id=",".join(video_ids),
                )
                .execute()
            )

            for item in video_response.get("items", []):
                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                content = item.get("contentDetails", {})
                status = item.get("status", {})

                video_data = {
                    "keyword": keyword,
                    "video_id": item["id"],
                    "title": snippet.get("title"),
                    "channel_title": snippet.get("channelTitle"),
                    "published_at": snippet.get("publishedAt"),
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                    "duration_seconds": convert_iso8601_duration(
                        content.get("duration")
                    ),
                    "definition": content.get("definition"),
                    "caption_available": content.get("caption") == "true",
                    "category_id": snippet.get("categoryId"),
                    "description": snippet.get("description"),
                    "tags": snippet.get("tags", []),
                    "default_language": snippet.get("defaultLanguage"),
                    "thumbnail_url": snippet.get("thumbnails", {})
                    .get("default", {})
                    .get("url"),
                    "privacy_status": status.get("privacyStatus"),
                    "live_broadcast_content": snippet.get("liveBroadcastContent"),
                    "url": f"https://www.youtube.com/watch?v={item['id']}",
                }

                videos.append(video_data)

            page_count += 1
            next_page_token = search_response.get("nextPageToken")
            if not next_page_token:
                break

        # Optional: sort videos by engagement
        videos.sort(key=lambda x: (x["like_count"], x["comment_count"]), reverse=True)

        result[keyword] = videos

    return result


# - Step 5: Load Data to S3 and MongoDB ----
def load_to_s3(videos_by_keyword):
    """Store videos in AWS S3 as separate CSVs per keyword."""
    s3 = boto3.client("s3")
    for keyword, videos in videos_by_keyword.items():
        if videos:
            df = pd.DataFrame(videos)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            clean_keyword = keyword.lower().replace(" ", "_")
            s3.put_object(
                Bucket="youtube-data-krc",
                Key=f"youtube_{clean_keyword}_videos.csv",
                Body=csv_buffer.getvalue(),
            )


def load_to_mongodb(videos_by_keyword):
    """Store videos in MongoDB with collection named after each keyword."""
    client = MongoClient(MONGO_URI)
    db = client["youtube_dataset"]

    for keyword, videos in videos_by_keyword.items():
        if videos:
            collection_name = keyword.lower().replace(" ", "_")
            collection = db[collection_name]
            collection.insert_many(videos)
