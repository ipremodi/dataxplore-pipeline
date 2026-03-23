import asyncio
import logging
import os
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Config ---
API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("TELEGRAM_PHONE")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# --- Paste your source channel usernames here ---
SOURCE_CHANNELS = [
    "XploreArchives", 
]

LIMIT_PER_CHANNEL = 50  # posts to pull per run


def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def insert_post(cursor, post: dict):
    cursor.execute("""
        INSERT INTO raw_posts (
            post_id, source_channel, content, has_media,
            media_url, source_views, source_forwards,
            ingested_at, raw_timestamp
        ) VALUES (
            %(post_id)s, %(source_channel)s, %(content)s, %(has_media)s,
            %(media_url)s, %(source_views)s, %(source_forwards)s,
            %(ingested_at)s, %(raw_timestamp)s
        )
        ON CONFLICT (post_id) DO NOTHING;
    """, post)


async def ingest_channel(client, channel_username: str, cursor):
    log.info(f"Ingesting: {channel_username}")
    try:
        async for message in client.iter_messages(channel_username, limit=LIMIT_PER_CHANNEL):
            if not message.text:
                continue

            has_media = message.media is not None
            media_url = None
            if isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)):
                media_url = f"https://t.me/{channel_username}/{message.id}"

            post = {
                "post_id": f"{channel_username}_{message.id}",
                "source_channel": channel_username,
                "content": message.text,
                "has_media": has_media,
                "media_url": media_url,
                "source_views": message.views or 0,
                "source_forwards": message.forwards or 0,
                "ingested_at": datetime.now(timezone.utc),
                "raw_timestamp": message.date,
            }
            insert_post(cursor, post)

        log.info(f"Done: {channel_username}")
    except Exception as e:
        log.error(f"Failed on {channel_username}: {e}")


async def main():
    conn = get_db_conn()
    conn.autocommit = False
    cursor = conn.cursor()

    async with TelegramClient("dataxplore_session", API_ID, API_HASH) as client:
        await client.start(phone=PHONE)
        for channel in SOURCE_CHANNELS:
            await ingest_channel(client, channel, cursor)

    conn.commit()
    cursor.close()
    conn.close()
    log.info("Ingestion complete.")


if __name__ == "__main__":
    asyncio.run(main())