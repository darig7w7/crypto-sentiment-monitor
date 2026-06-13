# TMDB Movie Metadata

Standalone script for looking up only the TMDB metadata needed by the Movie Quote Notifier project.

This project is intentionally independent from `movie-quote`. Later, Airflow can call the movie quote API in one task, call this TMDB lookup in another task, and merge both outputs before loading PostgreSQL.

## Setup

```bash
cp .env.example .env
```

Add your real TMDB read access token or API key to `.env`.

## Usage

With Docker Compose:

```bash
docker-compose run --rm tmdb "The Dark Knight"
```

With an optional year hint:

```bash
docker-compose run --rm tmdb "The Matrix" --year 1999
```

With a character from the quote API, so TMDB can try to return the actor:

```bash
docker-compose run --rm tmdb "The Dark Knight" --character "The Joker"
```

Local Python option:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/get_movie_metadata.py "The Dark Knight"
```

Example output shape:

```json
{
  "movie_query": "The Dark Knight",
  "match_found": true,
  "movie": {
    "tmdb_id": 155,
    "title": "The Dark Knight",
    "year": "2008",
    "genres": ["Action", "Crime", "Drama", "Thriller"],
    "rating": 8.5,
    "overview": "Batman raises the stakes in his war on crime...",
    "poster_url": "https://image.tmdb.org/t/p/w500/example.jpg"
  },
  "character": "The Joker",
  "actor": "Heath Ledger"
}
```
