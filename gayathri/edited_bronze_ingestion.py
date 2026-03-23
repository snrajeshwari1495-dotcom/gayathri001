import os
import time
import logging
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from googleapiclient.discovery import build

spark = SparkSession.builder.appName("Bronze_Ingestion").getOrCreate()

EV_API_URL = "https://data.wa.gov/resource/f6w7-q2d2.json"

YOUTUBE_KEYWORDS = [
    "tesla review",
    "nissan leaf review",
    "ev comparison",
    "electric car range"
]

BRONZE_EV = "data_lake/bronze/ev_population/"
BRONZE_YT = "data_lake/bronze/youtube_ev_sentiment/"

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

logging.basicConfig(
    filename="logs/bronze_ingestion.log",
    level=logging.INFO
)

def ingest_ev():

    offset = 0
    limit = 1000

    while True:

        params = {"$limit": limit, "$offset": offset}

        response = requests.get(EV_API_URL, params=params)

        # Check status
        if response.status_code != 200:
            print(" API ERROR:", response.status_code)
            print(response.text)
            break

        data = response.json()

        #  FIX: ensure it's list of dict
        if not isinstance(data, list):
            print(" Invalid API response:", data)
            break

        if len(data) == 0:
            break

        print(f" Fetched {len(data)} rows")

        df = spark.createDataFrame(data)

        df = df.withColumn(
            "ingestion_timestamp",
            lit(datetime.utcnow().isoformat())
        )

        df.write.mode("append").json(BRONZE_EV)

        offset += limit

        time.sleep(1)

def ingest_youtube():

    youtube = build(
        "youtube",
        "v3",
        developerKey=YOUTUBE_API_KEY
    )

    videos = []

    for keyword in YOUTUBE_KEYWORDS:

        req = youtube.search().list(
            q=keyword,
            part="snippet",
            type="video",
            maxResults=50
        )

        res = req.execute()

        for item in res["items"]:

            videos.append({
                "video_id": item["id"]["videoId"],
                "title": item["snippet"]["title"],
                "channel": item["snippet"]["channelTitle"],
                "published_at": item["snippet"]["publishedAt"],
                "keyword": keyword,
                "ingestion_timestamp":
                datetime.utcnow().isoformat()
            })

    if videos:

        df = spark.createDataFrame(videos)

        df.dropDuplicates(["video_id"]) \
          .write.mode("append") \
          .json(BRONZE_YT)


def main():

    logging.info("Starting Bronze ingestion")

    ingest_ev()

    ingest_youtube()

    logging.info("Bronze ingestion completed")

    print("Bronze ingestion finished")


if __name__ == "__main__":
    main()