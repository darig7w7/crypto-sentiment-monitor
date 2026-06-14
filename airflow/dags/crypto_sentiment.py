import json
import os
from datetime import datetime

import psycopg2
import requests
from airflow.decorators import dag, task
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
NEWSAPI_URL = "https://newsapi.org/v2/everything"
HACKERNEWS_URL = "https://hn.algolia.com/api/v1/search"

CRYPTO_KEYWORDS = [
    "bitcoin", "ethereum", "crypto", "blockchain", "btc", "eth",
    "defi", "nft", "altcoin", "stablecoin", "web3", "token",
    "wallet", "mining", "halving", "coinbase", "binance",
]

STOPWORDS = {
    "with", "that", "this", "from", "have", "been", "will", "their",
    "about", "would", "could", "after", "which", "there", "other",
    "more", "also", "when", "than", "bitcoin", "ethereum", "crypto",
}


def is_crypto_related(title: str, description: str) -> bool:
    text = f"{title} {description}".lower()
    return any(kw in text for kw in CRYPTO_KEYWORDS)


def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow",
    )


@dag(
    dag_id="crypto_sentiment_pipeline",
    description=(
        "Proyecto 8 — Fetch crypto prices, news articles and Hacker News posts, "
        "compute sentiment scores, and load results into PostgreSQL."
    ),
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["proyecto_8", "crypto", "sentiment"],
)
def crypto_sentiment_pipeline():

    @task(task_id="extract_crypto")
    def extract_crypto():
        response = requests.get(
            COINGECKO_URL,
            params={
                "ids": "bitcoin,ethereum",
                "vs_currencies": "usd",
                "include_24hr_change": "true",
            },
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        return {
            "bitcoin": {
                "price": data["bitcoin"]["usd"],
                "change_24h": data["bitcoin"]["usd_24h_change"],
            },
            "ethereum": {
                "price": data["ethereum"]["usd"],
                "change_24h": data["ethereum"]["usd_24h_change"],
            },
            "fetched_at": datetime.utcnow().isoformat(),
        }

    @task(task_id="extract_reddit")
    def extract_reddit():
        """
        Reddit's API rejects credentials for new accounts since 2023.
        This task is preserved to match the project architecture;
        its role is fulfilled by extract_news (NewsAPI).
        """
        return []

    @task(task_id="extract_news")
    def extract_news():
        response = requests.get(
            NEWSAPI_URL,
            params={
                "q": '"bitcoin" OR "ethereum" OR "crypto" OR "blockchain"',
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 10,
                "apiKey": NEWS_API_KEY,
            },
            timeout=20,
        )
        response.raise_for_status()
        articles = response.json().get("articles", [])
        return [
            {
                "title": a["title"],
                "description": a.get("description", ""),
                "source": a["source"]["name"],
                "published_at": a["publishedAt"],
            }
            for a in articles
            if is_crypto_related(a["title"], a.get("description", ""))
        ]

    @task(task_id="extract_hackernews")
    def extract_hackernews():
        response = requests.get(
            HACKERNEWS_URL,
            params={
                "query": "bitcoin ethereum crypto",
                "tags": "story",
                "hitsPerPage": 10,
            },
            timeout=20,
        )
        response.raise_for_status()
        hits = response.json().get("hits", [])
        return [
            {
                "title": h["title"],
                "url": h.get("url", ""),
                "points": h.get("points", 0),
                "created_at": h["created_at"],
            }
            for h in hits
            if h.get("title") and is_crypto_related(h["title"], "")
        ]

    @task(task_id="transform_crypto")
    def transform_crypto(prices):
        return prices

    @task(task_id="transform_reddit")
    def transform_reddit(reddit_data):
        return reddit_data

    @task(task_id="transform_news")
    def transform_news(news_data, hn_data):
        analyzer = SentimentIntensityAnalyzer()
        results = []

        for article in news_data:
            text = f"{article['title']} {article['description']}"
            score = analyzer.polarity_scores(text)
            results.append({
                "source": article["source"],
                "title": article["title"],
                "compound": score["compound"],
                "type": "news",
            })

        for post in hn_data:
            score = analyzer.polarity_scores(post["title"])
            results.append({
                "source": "HackerNews",
                "title": post["title"],
                "compound": score["compound"],
                "type": "hackernews",
            })

        return results

    @task(task_id="combine_data")
    def combine_data(sentiment_items):
        avg = (
            sum(r["compound"] for r in sentiment_items) / len(sentiment_items)
            if sentiment_items
            else 0
        )
        return {
            "items": sentiment_items,
            "average_sentiment": round(avg, 4),
            "total_analyzed": len(sentiment_items),
        }

    @task(task_id="load_postgres")
    def load_postgres(prices, sentiment):
        fetched_at = datetime.utcnow()
        conn = get_db_connection()
        cur = conn.cursor()

        for coin, data in prices.items():
            if coin == "fetched_at":
                continue
            cur.execute(
                """
                INSERT INTO crypto_prices (fetched_at, coin, price_usd, change_24h)
                VALUES (%s, %s, %s, %s)
                """,
                (fetched_at, coin, data["price"], data["change_24h"]),
            )

        cur.execute(
            """
            INSERT INTO crypto_sentiment (fetched_at, average_sentiment, label, total_analyzed)
            VALUES (%s, %s, %s, %s)
            """,
            (
                fetched_at,
                sentiment["average_sentiment"],
                (
                    "positive" if sentiment["average_sentiment"] > 0.05
                    else "negative" if sentiment["average_sentiment"] < -0.05
                    else "neutral"
                ),
                sentiment["total_analyzed"],
            ),
        )

        for item in sentiment["items"]:
            cur.execute(
                """
                INSERT INTO sentiment_items (fetched_at, source, title, compound, type)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (title) DO NOTHING
                """,
                (fetched_at, item["source"], item["title"], item["compound"], item["type"]),
            )

        conn.commit()
        cur.close()
        conn.close()

        print(json.dumps({
            "fetched_at": str(fetched_at),
            "bitcoin_price": prices["bitcoin"]["price"],
            "average_sentiment": sentiment["average_sentiment"],
            "total_analyzed": sentiment["total_analyzed"],
        }, indent=2))

    crypto_data    = extract_crypto()
    reddit_data    = extract_reddit()
    news_data      = extract_news()
    hn_data        = extract_hackernews()

    clean_crypto   = transform_crypto(crypto_data)
    clean_reddit   = transform_reddit(reddit_data)
    clean_text     = transform_news(news_data, hn_data)

    combined       = combine_data(clean_text)

    load_postgres(clean_crypto, combined)


crypto_sentiment_pipeline()