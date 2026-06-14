# Crypto Sentiment Pipeline

Data Engineering lab project (Proyecto 8) using Docker, Airflow, CoinGecko, NewsAPI, Hacker News, VADER sentiment analysis, and PostgreSQL.

The pipeline combines prices and text from three sources to compute sentiment scores and correlate them with price movement:

```text
CoinGecko API ───────> transform_crypto ──────┐
                                              │
NewsAPI (Reddit alt.) ──> transform_news ─────┼──> combine_data ──> load_postgres
                                              │
Hacker News API ────────> transform_news ─────┘
```

## Folders

```text
airflow/
```

Airflow environment with the DAG `crypto_sentiment_pipeline`. Runs every 15 minutes, extracts prices and text, computes sentiment scores, and loads results into PostgreSQL.

```text
dashboard/
```

Flask API and HTML dashboard that reads from PostgreSQL and visualizes prices, sentiment over time, keyword frequency, and price movement correlation.

```text
project-idea.md
project-idea.es.md
```

Project idea document in English and Spanish.

## Requirements

- Docker / Docker Compose
- Python 3.10+
- NewsAPI key (free at newsapi.org)

## Design Decision: NewsAPI instead of Reddit

Reddit's API rejects credentials for new accounts since 2023. NewsAPI provides articles from specialized crypto media such as CoinDesk and Cointelegraph. The `extract_reddit` task is preserved in the DAG to match the project architecture, with a documented note explaining the substitution.

## 1. Configure environment

```bash
cp tmdb-movie-metadata/.env.example tmdb-movie-metadata/.env
```

Edit `tmdb-movie-metadata/.env` and add your NewsAPI key:

```env
NEWS_API_KEY=your_newsapi_key_here
```

## 2. Start Airflow

```bash
cd airflow
sudo chown -R 50000:0 logs/
docker compose up -d
```

Open Airflow:

```text
http://localhost:8081
```

Login:

```text
admin / admin
```

Note: Airflow takes 2–3 minutes to initialize on first run. The webserver runs on port `8081`, not `8080`.

## 3. Create PostgreSQL tables

```bash
docker exec -it airflow-postgres-1 psql -U airflow -d airflow
```

```sql
CREATE TABLE crypto_prices (
    id SERIAL PRIMARY KEY,
    fetched_at TIMESTAMP,
    coin VARCHAR(20),
    price_usd NUMERIC,
    change_24h NUMERIC
);

CREATE TABLE crypto_sentiment (
    id SERIAL PRIMARY KEY,
    fetched_at TIMESTAMP,
    average_sentiment NUMERIC,
    label VARCHAR(20),
    total_analyzed INTEGER
);

CREATE TABLE sentiment_items (
    id SERIAL PRIMARY KEY,
    fetched_at TIMESTAMP,
    source VARCHAR(100),
    title TEXT UNIQUE,
    compound NUMERIC,
    type VARCHAR(20)
);
```

## 4. Run the DAG

In the Airflow UI:

1. Open `crypto_sentiment_pipeline`
2. Unpause the DAG
3. Trigger it manually
4. Check task logs

Task flow:

```text
extract_crypto ─────────────────────────────────────────┐
extract_reddit ─────────────────────────────────────────┤
extract_news ───┐                                       │
                ├──> transform_news ──> combine_data ───┼──> load_postgres
extract_hackernews ─┘                                   │
                                        transform_crypto─┘
```

Pipeline steps:

1. Extract crypto prices
2. Extract Reddit discussions (preserved, fulfilled by NewsAPI)
3. Extract Hacker News posts
4. Transform datasets
5. Compute sentiment scores
6. Combine datasets
7. Load into PostgreSQL

The DAG runs automatically every 15 minutes after the first trigger.

The final payload is printed in the `load_postgres` task logs:

```json
{
  "fetched_at": "2026-06-14 02:00:07",
  "bitcoin_price": 64600,
  "average_sentiment": 0.1541,
  "total_analyzed": 15
}
```

## 5. Start the dashboard

```bash
sudo apt install python3-venv python3-full -y

cd dashboard
python3 -m venv venv
source venv/bin/activate
pip install flask flask-cors psycopg2-binary
python app.py
```

Dashboard available at:

```text
http://127.0.0.1:5000
```

## Business Questions

**What discussions happen before price increases?**

```sql
SELECT
    cp1.fetched_at,
    cp1.price_usd AS precio_actual,
    cp2.price_usd - cp1.price_usd AS variacion,
    cs.average_sentiment,
    cs.label
FROM crypto_prices cp1
JOIN crypto_prices cp2
    ON cp1.coin = cp2.coin
    AND cp2.fetched_at = (
        SELECT MIN(fetched_at) FROM crypto_prices
        WHERE fetched_at > cp1.fetched_at AND coin = cp1.coin
    )
JOIN crypto_sentiment cs
    ON DATE_TRUNC('minute', cs.fetched_at) = DATE_TRUNC('minute', cp1.fetched_at)
WHERE cp1.coin = 'bitcoin'
ORDER BY cp1.fetched_at ASC;
```

**What words appear most frequently?**

```sql
SELECT word, COUNT(*) AS frecuencia
FROM (
    SELECT regexp_split_to_table(lower(title), '\s+') AS word
    FROM sentiment_items
) words
WHERE length(word) > 4
AND word NOT IN (
    'with','that','this','from','have','been','will','their',
    'about','would','could','after','bitcoin','ethereum','crypto'
)
GROUP BY word
ORDER BY frecuencia DESC
LIMIT 20;
```

**Does sentiment correlate with price movement?**

```sql
SELECT
    cs.fetched_at,
    cs.average_sentiment,
    cs.label,
    cp.coin,
    cp.price_usd,
    cp.change_24h
FROM crypto_sentiment cs
JOIN crypto_prices cp
    ON DATE_TRUNC('minute', cs.fetched_at) = DATE_TRUNC('minute', cp.fetched_at)
ORDER BY cs.fetched_at ASC;
```

## Stop Everything

```bash
cd airflow
docker compose down
```

## Troubleshooting

If Airflow is not visible on `8081`:

```bash
cd airflow
docker compose ps
docker compose logs --tail=50 airflow-webserver
```

If the webserver fails with permission errors on logs:

```bash
cd airflow
sudo chown -R 50000:0 logs/
docker compose down -v
docker compose up -d
```

If PostgreSQL is not reachable from the dashboard, confirm the port is exposed:

```bash
docker compose ps
# postgres should show 0.0.0.0:5432->5432/tcp
```

If the DAG shows import errors, confirm `vaderSentiment` is listed in `_PIP_ADDITIONAL_REQUIREMENTS` inside `docker-compose.yml`:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: requests==2.31.0 psycopg2-binary==2.9.9 vaderSentiment==3.3.2
```