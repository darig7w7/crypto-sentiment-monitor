# Movie Quote Notifier

Data Engineering lab project using Docker, Airflow, a local movie quotes API, TMDB metadata, PostgreSQL, and macOS notifications.

The project combines two data sources:

```text
movie-quote API ───────> quote + movie + character ──┐
                                                      ├──> Airflow DAG ──> merged payload ──> notification files
TMDB API ──────────────> poster + year + genre + rating + overview + actor ─┘
```

## Folders

```text
movie-quote/
```

Local Django REST API that returns movie quotes. It includes Docker Compose and sample quote data.

```text
tmdb-movie-metadata/
```

Standalone TMDB lookup tool. Given a movie name, it returns the movie poster, year, genres, rating, overview, and actor when a character is provided.

```text
airflow/
```

Airflow environment with a DAG that fetches a quote, fetches TMDB metadata, merges both payloads, and writes notification files.

```text
project-idea.md
project-idea.es.md
```

Project idea document in English and Spanish.

## Requirements

- Docker / Docker Compose
- TMDB API read access token or API key
- macOS, if you want local notifications
- Optional: `terminal-notifier` for richer macOS notifications

```bash
brew install terminal-notifier
```

## 1. Start the Movie Quote API

```bash
cd movie-quote
docker-compose up --build -d
```

The API runs at:

```text
http://127.0.0.1:8001
```

Useful endpoints:

```text
http://127.0.0.1:8001/v1/quote/
http://127.0.0.1:8001/v1/shows/
http://127.0.0.1:8001/v1/shows/top-gun-maverick/
```

## 2. Configure TMDB

```bash
cd ../tmdb-movie-metadata
cp .env.example .env
```

Edit `.env` and add your real credential:

```env
TMDB_READ_ACCESS_TOKEN=your_real_token
```

You can test the TMDB tool with Docker:

```bash
docker-compose run --rm tmdb "The Dark Knight" --character "The Joker"
```

Expected output shape:

```json
{
  "movie_query": "The Dark Knight",
  "match_found": true,
  "movie": {
    "title": "The Dark Knight",
    "year": "2008",
    "genres": ["Action", "Crime", "Thriller"],
    "rating": 8.5,
    "overview": "...",
    "poster_url": "https://image.tmdb.org/t/p/w500/..."
  },
  "character": "The Joker",
  "actor": "Heath Ledger"
}
```

## 3. Start Airflow

```bash
cd ../airflow
cp .env.example .env
docker-compose up airflow-init
docker-compose up -d
```

Open Airflow:

```text
http://127.0.0.1:8081
```

Login:

```text
admin / admin
```

The example DAG is:

```text
movie_quote_notifier_example
```

## 4. Run the DAG

In the Airflow UI:

1. Open `movie_quote_notifier_example`
2. Unpause the DAG
3. Trigger it manually
4. Check task logs

Task flow:

```text
fetch_quote
  -> fetch_tmdb_metadata
  -> merge_quote_and_movie
  -> notification_preview
```

The final merged payload is printed in the `merge_quote_and_movie` task logs.

The notification payload is written to:

```text
airflow/notifications/latest_notification.json
airflow/notifications/latest_poster.jpg
```

## 5. Show macOS Notifications

To show the latest notification once:

```bash
cd airflow
./scripts/show_latest_notification.sh
```

To keep a watcher running forever:

```bash
./scripts/watch_notifications.sh
```

Leave that terminal open. Every time Airflow writes a new notification payload, the watcher shows it on your Mac.

Clicking the notification opens the downloaded poster image.

## How Data Moves Between Airflow Tasks

The DAG uses Airflow TaskFlow API.

```python
quote = fetch_quote()
tmdb = fetch_tmdb_metadata(quote)
merged = merge_quote_and_movie(quote, tmdb)
notification_preview(merged)
```

Each `return` value is stored by Airflow as an XCom. When the next task receives that value as a parameter, Airflow automatically creates the dependency and passes the data.

Conceptually:

```text
fetch_quote returns quote_payload
        ↓
fetch_tmdb_metadata receives quote_payload and returns tmdb_payload
        ↓
merge_quote_and_movie receives quote_payload + tmdb_payload and returns merged_payload
        ↓
notification_preview receives merged_payload and writes notification files
```

## Stop Everything

```bash
cd airflow
docker-compose down

cd ../movie-quote
docker-compose down
```

The TMDB helper uses one-off containers, so it usually does not need to be stopped.

## Troubleshooting

If Airflow is not visible on `8081`:

```bash
cd airflow
docker-compose ps
docker-compose logs --tail=100 airflow-webserver
```

If `movie-quote` does not respond:

```bash
cd movie-quote
docker-compose ps
curl http://127.0.0.1:8001/v1/quote/
```

If TMDB fails inside Airflow, confirm this file exists and contains your real token:

```text
tmdb-movie-metadata/.env
```

Then restart Airflow:

```bash
cd airflow
docker-compose up -d --force-recreate airflow-webserver airflow-scheduler
```

If macOS notifications do not appear, check notification permissions for your terminal app in System Settings.
