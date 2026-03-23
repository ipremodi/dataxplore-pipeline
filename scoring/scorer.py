import json
import logging
import os
import time
from datetime import datetime, timezone

from google import genai
from google.genai import types

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

SCORING_PROMPT = """
You are a content evaluator for DataXplore, a Telegram channel strictly 
focused on core-level AI/ML engineering content. The channel covers: 
custom LLM architecture, transformer internals, fine-tuning, RAG pipelines, 
training infrastructure, embeddings, and ML systems engineering.

The channel does NOT cover: prompt engineering, surface-level ChatGPT 
tutorials, AI news/hype, no-code AI tools, or general Python tutorials.

Evaluate the following post on three dimensions, each scored 0-10:

1. TECHNICAL DEPTH - Does it go below surface level? Does it involve 
   implementation, architecture decisions, math, or systems thinking?
   (0 = surface fluff, 10 = deep technical insight)

2. RELEVANCE - Does it fit DataXplore's strict positioning around core 
   ML/AI engineering? Not adjacent topics, not AI business news.
   (0 = completely off-topic, 10 = perfect fit)

3. NOVELTY - Is this covering something not commonly known or recently 
   covered? Is it fresh signal or recycled content?
   (0 = seen everywhere, 10 = genuinely new insight)

Return ONLY a valid JSON object, no markdown, no explanation:
{
  "depth_score": float,
  "relevance_score": float,
  "novelty_score": float,
  "topic_tag": "transformers|RAG|fine-tuning|infra|paper|tool|other",
  "reject_reason": "string or null"
}

POST CONTENT:
"""


def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def fetch_unscored_posts(cursor):
    cursor.execute("""
        SELECT r.post_id, r.content
        FROM raw_posts r
        LEFT JOIN processed_posts p ON r.post_id = p.post_id
        WHERE p.post_id IS NULL
        AND r.content IS NOT NULL
        AND LENGTH(r.content) > 50
        ORDER BY r.ingested_at DESC;
    """)
    return cursor.fetchall()


def score_post(content: str) -> dict | None:
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=SCORING_PROMPT + content[:2000])
        raw = response.text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as e:
        log.error(f"Scoring failed: {e}")
        return None


def compute_status(score: float) -> str:
    if score >= 7.5:
        return "approved"
    elif score >= 5.0:
        return "manual_review"
    else:
        return "rejected"


def insert_processed(cursor, post_id: str, scores: dict):
    composite = round(
        scores["depth_score"] * 0.4 +
        scores["relevance_score"] * 0.4 +
        scores["novelty_score"] * 0.2, 2
    )
    status = compute_status(composite)
    cursor.execute("""
        INSERT INTO processed_posts (
            post_id, is_duplicate, topic_tag,
            relevance_score, depth_score, novelty_score,
            composite_score, status, processed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (post_id) DO NOTHING;
    """, (
        post_id, False, scores.get("topic_tag", "other"),
        scores["relevance_score"], scores["depth_score"],
        scores["novelty_score"], composite, status,
        datetime.now(timezone.utc)
    ))
    return composite, status


def main():
    conn = get_db_conn()
    cursor = conn.cursor()

    posts = fetch_unscored_posts(cursor)
    log.info(f"Found {len(posts)} unscored posts")

    for i, (post_id, content) in enumerate(posts):
        log.info(f"Scoring [{i+1}/{len(posts)}]: {post_id}")
        scores = score_post(content)
        if scores:
            composite, status = insert_processed(cursor, post_id, scores)
            log.info(f"  → composite={composite} status={status}")
            conn.commit()
        time.sleep(1)  # rate limit buffer

    cursor.close()
    conn.close()
    log.info("Scoring complete.")


if __name__ == "__main__":
    main()