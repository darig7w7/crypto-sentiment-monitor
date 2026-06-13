import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

import requests
from airflow.decorators import dag, task


QUOTE_API_URL = os.environ.get(
    "QUOTE_API_URL",
    "http://host.docker.internal:8001/v1/quote/",
)
QUOTE_API_HOST_HEADER = os.environ.get("QUOTE_API_HOST_HEADER", "127.0.0.1:8001")
TMDB_SCRIPT_PATH = os.environ.get(
    "TMDB_SCRIPT_PATH",
    "/opt/airflow/sources/tmdb-movie-metadata/scripts/get_movie_metadata.py",
)
NOTIFICATION_DIR = Path(os.environ.get("NOTIFICATION_DIR", "/opt/airflow/notifications"))


@dag(
    dag_id="movie_quote_notifier_example",
    description="Fetch a movie quote, fetch TMDB metadata, and merge both payloads.",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["movie-quotes", "tmdb", "example"],
)
def movie_quote_notifier_example():
    @task
    def fetch_quote():
        response = requests.get(
            QUOTE_API_URL,
            headers={"Host": QUOTE_API_HOST_HEADER},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") == "no_quote":
            raise ValueError("Movie quote API returned no quote data.")

        return {
            "quote": payload["quote"],
            "movie": payload["show"],
            "character": payload.get("role"),
            "contain_adult_lang": payload.get("contain_adult_lang", False),
        }

    @task
    def fetch_tmdb_metadata(quote_payload):
        command = [
            "python",
            TMDB_SCRIPT_PATH,
            quote_payload["movie"],
        ]
        if quote_payload.get("character"):
            command.extend(["--character", quote_payload["character"]])

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                "TMDB metadata script failed.\n"
                f"Command: {' '.join(command)}\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )
        return json.loads(result.stdout)

    @task
    def merge_quote_and_movie(quote_payload, tmdb_payload):
        movie = tmdb_payload.get("movie") or {}
        merged = {
            "quote": quote_payload["quote"],
            "movie": quote_payload["movie"],
            "character": quote_payload.get("character"),
            "actor": tmdb_payload.get("actor"),
            "poster_url": movie.get("poster_url"),
            "year": movie.get("year"),
            "genres": movie.get("genres", []),
            "rating": movie.get("rating"),
            "overview": movie.get("overview"),
            "tmdb_id": movie.get("tmdb_id"),
        }

        print(json.dumps(merged, indent=2, ensure_ascii=False))
        return merged

    @task
    def notification_preview(merged_payload):
        NOTIFICATION_DIR.mkdir(parents=True, exist_ok=True)
        poster_path = None
        if merged_payload.get("poster_url"):
            poster_response = requests.get(merged_payload["poster_url"], timeout=30)
            poster_response.raise_for_status()
            poster_path = NOTIFICATION_DIR / "latest_poster.jpg"
            poster_path.write_bytes(poster_response.content)

        message = (
            "Quote of the hour\n\n"
            f"{merged_payload['movie']} ({merged_payload.get('year') or 'unknown year'})\n"
            f"Rating: {merged_payload.get('rating') or 'N/A'}\n\n"
            f"\"{merged_payload['quote']}\"\n"
            f"- {merged_payload.get('character') or 'Unknown character'}"
        )
        if merged_payload.get("actor"):
            message += f" ({merged_payload['actor']})"
        if merged_payload.get("poster_url"):
            message += f"\n\nPoster: {merged_payload['poster_url']}"

        notification_payload = {
            **merged_payload,
            "message": message,
            "poster_file": str(poster_path) if poster_path else None,
        }
        payload_path = NOTIFICATION_DIR / "latest_notification.json"
        payload_path.write_text(
            json.dumps(notification_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        print(message)
        print(f"\nmacOS notification payload: {payload_path}")
        return message

    quote = fetch_quote()
    tmdb = fetch_tmdb_metadata(quote)
    merged = merge_quote_and_movie(quote, tmdb)
    notification_preview(merged)


movie_quote_notifier_example()
