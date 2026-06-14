import json
import os
from datetime import datetime

import psycopg2
import requests
from airflow.decorators import dag, task
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")

CRYPTO_KEYWORDS = [
    "bitcoin", "ethereum", "crypto", "blockchain", "btc", "eth",
    "coinbase", "binance", "defi", "nft", "altcoin", "stablecoin",
    "web3", "token", "wallet", "mining", "halving"
]

def is_crypto_related(title, description):
    text = f"{title} {description}".lower()
    return any(keyword in text for keyword in CRYPTO_KEYWORDS)

def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow",
    )

@dag(
    dag_id="crypto_sentiment",
    description="Fetch crypto prices, news and HackerNews posts, analyze sentiment.",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["crypto", "sentiment", "news"],
)
def crypto_sentiment():

    @task
    def fetch_prices():
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "bitcoin,ethereum",
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        }
        response = requests.get(url, params=params, timeout=20)
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

    @task
    def fetch_news():
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": "bitcoin OR ethereum OR crypto",
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 10,
            "apiKey": NEWS_API_KEY,
        }
        response = requests.get(url, params=params, timeout=20)
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

    @task
    def fetch_hackernews():
        search_url = "https://hn.algolia.com/api/v1/search"
        params = {
            "query": "bitcoin ethereum crypto",
            "tags": "story",
            "hitsPerPage": 10,
        }
        response = requests.get(search_url, params=params, timeout=20)
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
        ]

    @task
    def analyze_sentiment(news_articles, hn_posts):
        analyzer = SentimentIntensityAnalyzer()
        results = []

        for article in news_articles:
            text = f"{article['title']} {article['description']}"
            score = analyzer.polarity_scores(text)
            results.append({
                "source": article["source"],
                "title": article["title"],
                "compound": score["compound"],
                "type": "news",
            })

        for post in hn_posts:
            score = analyzer.polarity_scores(post["title"])
            results.append({
                "source": "HackerNews",
                "title": post["title"],
                "compound": score["compound"],
                "type": "hackernews",
            })

        avg_sentiment = sum(r["compound"] for r in results) / len(results) if results else 0
        return {
            "items": results,
            "average_sentiment": round(avg_sentiment, 4),
            "total_analyzed": len(results),
        }

    @task
    def save_to_postgres(prices, sentiment):
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
                "positive" if sentiment["average_sentiment"] > 0.05
                else "negative" if sentiment["average_sentiment"] < -0.05
                else "neutral",
                sentiment["total_analyzed"],
            ),
        )

        for item in sentiment["items"]:
            cur.execute(
                """
                INSERT INTO sentiment_items (fetched_at, source, title, compound, type)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (fetched_at, item["source"], item["title"], item["compound"], item["type"]),
            )

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Guardado: Bitcoin ${prices['bitcoin']['price']} | Sentimiento: {sentiment['average_sentiment']}")

    prices = fetch_prices()
    news = fetch_news()
    hn = fetch_hackernews()
    sentiment = analyze_sentiment(news, hn)
    save_to_postgres(prices, sentiment)

crypto_sentiment()