import argparse
import json
import os
import sys
from urllib.parse import urljoin

import requests


TMDB_BASE_URL = "https://api.themoviedb.org/3/"
TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p"


def load_dotenv(path=".env"):
    if not os.path.exists(path):
        return

    with open(path) as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key, value.strip().strip('"').strip("'"))


def has_value(name, placeholder):
    value = os.environ.get(name)
    return bool(value and value != placeholder)


def ensure_tmdb_credentials():
    has_token = has_value("TMDB_READ_ACCESS_TOKEN", "your_tmdb_read_access_token_here")
    has_api_key = has_value("TMDB_API_KEY", "your_tmdb_api_key_here")
    if not has_token and not has_api_key:
        raise RuntimeError("Set TMDB_READ_ACCESS_TOKEN or TMDB_API_KEY in .env.")


def tmdb_headers():
    if has_value("TMDB_READ_ACCESS_TOKEN", "your_tmdb_read_access_token_here"):
        return {"Authorization": f"Bearer {os.environ['TMDB_READ_ACCESS_TOKEN']}"}
    return {}


def tmdb_params(params=None):
    params = dict(params or {})
    if has_value("TMDB_API_KEY", "your_tmdb_api_key_here"):
        params["api_key"] = os.environ["TMDB_API_KEY"]
    return params


def tmdb_get(path, params=None):
    response = requests.get(
        urljoin(TMDB_BASE_URL, path),
        headers=tmdb_headers(),
        params=tmdb_params(params),
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def search_movie(movie_name, year=None):
    params = {"query": movie_name, "include_adult": "false"}
    if year:
        params["year"] = year

    data = tmdb_get("search/movie", params)
    results = data.get("results", [])
    if not results:
        return None

    movie_name_normalized = movie_name.lower()
    exact_matches = [
        result for result in results
        if (result.get("title") or "").lower() == movie_name_normalized
        or (result.get("original_title") or "").lower() == movie_name_normalized
    ]
    if exact_matches:
        return sorted(exact_matches, key=lambda item: item.get("release_date") or "9999-99-99")[0]

    return results[0]


def get_movie_details(movie_id):
    return tmdb_get(f"movie/{movie_id}", {"append_to_response": "credits"})


def poster_url(path):
    if not path:
        return None
    return f"{TMDB_IMAGE_BASE_URL}/w500{path}"


def find_actor(details, character):
    if not character:
        return None

    character_normalized = character.lower()
    for cast_member in details.get("credits", {}).get("cast", []):
        cast_character = cast_member.get("character", "").lower()
        if character_normalized in cast_character or cast_character in character_normalized:
            return cast_member.get("name")
    return None


def movie_payload(query, movie_match, character=None):
    if not movie_match:
        return {
            "movie_query": query,
            "match_found": False,
            "movie": None,
            "character": character,
            "actor": None,
        }

    details = get_movie_details(movie_match["id"])
    release_date = details.get("release_date") or ""

    return {
        "movie_query": query,
        "match_found": True,
        "movie": {
            "tmdb_id": details.get("id"),
            "title": details.get("title"),
            "year": release_date[:4] or None,
            "genres": [genre.get("name") for genre in details.get("genres", [])],
            "rating": details.get("vote_average"),
            "overview": details.get("overview"),
            "poster_url": poster_url(details.get("poster_path")),
        },
        "character": character,
        "actor": find_actor(details, character),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Get TMDB metadata for a movie name.")
    parser.add_argument("movie_name", help="Movie title to search in TMDB.")
    parser.add_argument("--year", help="Optional release year to improve matching.")
    parser.add_argument(
        "--character",
        help="Optional character name from the quote API. Used to find the actor in TMDB credits.",
    )
    return parser.parse_args()


def main():
    load_dotenv()
    args = parse_args()

    try:
        ensure_tmdb_credentials()
        movie_match = search_movie(args.movie_name, args.year)
        payload = movie_payload(args.movie_name, movie_match, args.character)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    except requests.HTTPError as exc:
        print(f"HTTP error: {exc.response.status_code} {exc.response.text}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
