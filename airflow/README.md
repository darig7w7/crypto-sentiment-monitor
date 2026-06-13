# Airflow

Local Airflow workspace for orchestrating the Movie Quote Notifier pipeline.

This Compose setup starts:

- PostgreSQL for Airflow metadata
- Airflow init
- Airflow webserver
- Airflow scheduler

The sibling projects are mounted read-only inside Airflow:

- `../movie-quote` at `/opt/airflow/sources/movie-quote`
- `../tmdb-movie-metadata` at `/opt/airflow/sources/tmdb-movie-metadata`

## Setup

```bash
cp .env.example .env
```

On macOS, `AIRFLOW_UID=50000` is fine. On Linux, you may prefer:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

## Start Airflow

```bash
docker-compose up airflow-init
docker-compose up -d
```

Open:

```text
http://127.0.0.1:8081
```

Default login:

```text
username: admin
password: admin
```

## Stop Airflow

```bash
docker-compose down
```

To remove Airflow metadata database volume too:

```bash
docker-compose down -v
```

## DAGs

Put DAG files in:

```text
airflow/dags/
```

This workspace includes an example DAG:

```text
movie_quote_notifier_example
```

Before running it, make sure your TMDB token or API key exists in `../tmdb-movie-metadata/.env`:

```env
TMDB_READ_ACCESS_TOKEN=your_real_token
```

Make sure `movie-quote` is running too:

```bash
cd ../movie-quote
docker-compose up -d
```

Later, the pipeline can have tasks like:

1. Fetch quote from `movie-quote`
2. Fetch TMDB metadata by movie name
3. Merge quote + movie data
4. Load PostgreSQL analytical tables
5. Trigger local notification

## macOS notification bridge

Docker containers cannot directly show native macOS notifications. The example DAG writes the latest notification payload and poster to:

```text
airflow/notifications/
```

After a DAG run, show the latest notification on your Mac:

```bash
./scripts/show_latest_notification.sh
```

Or keep a watcher running before you trigger the DAG:

```bash
./scripts/watch_notifications.sh
```

By default, the watcher ignores the already-existing notification and waits for the next DAG update. To show the current notification immediately when the watcher starts:

```bash
SHOW_EXISTING=1 ./scripts/watch_notifications.sh
```

If `terminal-notifier` is installed, the poster appears inside the notification. Otherwise the script sends a text notification with `osascript` and opens the poster image separately.
