# Airflow Project Ideas for Data Engineering Students

## Introduction

One of the best ways to learn Apache Airflow is by building projects that consume real-world data from APIs, transform that data, and load it into a database for analysis.

The goal of these projects is not only to learn Airflow, but also to understand fundamental Data Engineering concepts such as:

* Data ingestion
* Data transformation
* Data enrichment
* Data orchestration
* Data warehousing
* Scheduling and monitoring
* ETL and ELT pipelines
* Working with external APIs
* Data quality validation

All projects in this document follow a similar architecture:

```text
Source A ──> Transform A ──┐
                           ├──> Combine ──> Data Warehouse
Source B ──> Transform B ──┘
```

This structure helps students understand:

* Parallel tasks
* Task dependencies
* DAG orchestration
* Retry policies
* Logging and monitoring
* Scheduling
* Data modeling

---

# Project 1: News and Cryptocurrency Analytics

## Description

Build a pipeline that combines cryptocurrency prices with technology and finance news.

This project introduces students to working with multiple APIs and correlating information from different sources.

## Architecture

```text
News API ──────────────> Clean News ──────┐
                                          ├──> Combine ──> PostgreSQL
CoinGecko API ────────> Clean Prices ────┘
```

## Airflow Schedule

Every 10 minutes.

## Pipeline Steps

1. Extract news articles
2. Extract cryptocurrency prices
3. Clean and normalize news
4. Clean and normalize price data
5. Combine datasets
6. Load into PostgreSQL
7. Generate analytical tables

## Business Questions

* What news appears when Bitcoin increases in price?
* Which topics are trending?
* How many AI-related articles appear each day?
* How does Bitcoin behave throughout the day?

## Suggested DAG

```text
crypto_news_pipeline

extract_news
extract_crypto

transform_news
transform_crypto

combine_data

load_postgres

create_analytics_table
```

---

# Project 2: GitHub Analytics

## Description

Build a pipeline that tracks GitHub repositories and development activity.

This project is especially relevant for software engineering students because they already understand repositories, commits, and contributors.

## Architecture

```text
GitHub Repositories API ──> Clean Repos ───┐
                                            ├──> Combine ──> PostgreSQL
GitHub Commits API ───────> Clean Commits ─┘
```

## Airflow Schedule

Every hour.

## Pipeline Steps

1. Extract popular repositories
2. Extract recent commits
3. Transform repository data
4. Transform commit data
5. Combine datasets
6. Load into PostgreSQL

## Business Questions

* Which repositories are growing the fastest?
* Which programming languages are most popular?
* How many commits occur daily?
* Which repositories receive the most activity?

## Suggested DAG

```text
github_pipeline

extract_repositories
extract_commits

transform_repositories
transform_commits

combine_github_data

load_postgres
```

---

# Project 3: Technology Job Market Analytics

## Description

Build a pipeline that analyzes remote job opportunities and trending technologies.

This project introduces real-world labor market analytics.

## Architecture

```text
RemoteOK API ──────────> Clean Jobs ──────┐
                                          ├──> Combine ──> PostgreSQL
GitHub Trending Data ──> Clean Skills ───┘
```

## Airflow Schedule

Every 6 hours.

## Pipeline Steps

1. Extract job listings
2. Extract trending technologies
3. Normalize skills
4. Match jobs with technologies
5. Load into PostgreSQL

## Business Questions

* Which technologies are most in demand?
* Which jobs offer the highest salaries?
* Which skills commonly appear together?

## Suggested DAG

```text
job_market_pipeline

extract_jobs
extract_trending_skills

transform_jobs
transform_skills

combine_data

load_postgres
```

---

# Project 4: Weather and Air Quality Monitoring

## Description

Build a pipeline that combines weather information with air quality metrics.

This project introduces environmental data engineering and public datasets.

## Architecture

```text
Open-Meteo API ───────> Clean Weather ────┐
                                           ├──> Combine ──> PostgreSQL
OpenAQ API ───────────> Clean Air Data ───┘
```

## Airflow Schedule

Every hour.

## Pipeline Steps

1. Extract weather data
2. Extract air quality data
3. Transform weather data
4. Transform air quality data
5. Combine datasets
6. Load into PostgreSQL

## Business Questions

* When is pollution highest?
* How does temperature affect pollution?
* Which cities have the best air quality?

## Suggested DAG

```text
weather_air_pipeline

extract_weather
extract_air_quality

transform_weather
transform_air_quality

combine_data

load_postgres
```

---

# Project 5: NBA Analytics

## Description

Build a sports analytics pipeline using NBA data.

This project is engaging for students interested in sports and demonstrates how professional analytics systems work.

## Architecture

```text
NBA Scores API ───────> Clean Games ──────┐
                                           ├──> Combine ──> PostgreSQL
NBA Players API ──────> Clean Players ────┘
```

## Airflow Schedule

Daily.

## Pipeline Steps

1. Extract game results
2. Extract player statistics
3. Transform game data
4. Transform player data
5. Combine datasets
6. Load into PostgreSQL

## Business Questions

* Which player scores the most points?
* Which team has the longest winning streak?
* Which players are improving over time?

## Suggested DAG

```text
nba_pipeline

extract_games
extract_players

transform_games
transform_players

combine_data

load_postgres
```

---

# Project 6: Real-Time Flight Tracking

## Description

Build a pipeline that tracks aircraft activity around the world.

This project introduces near real-time ingestion and location-based datasets.

## Architecture

```text
OpenSky API ──────────> Clean Flights ────┐
                                           ├──> Combine ──> PostgreSQL
Airport Database ─────> Clean Airports ───┘
```

## Airflow Schedule

Every 5 minutes.

## Pipeline Steps

1. Extract flight information
2. Extract airport information
3. Transform flight data
4. Transform airport data
5. Combine datasets
6. Load into PostgreSQL

## Business Questions

* Which routes are most active?
* Which countries have the highest traffic?
* How many flights are currently over South America?

## Suggested DAG

```text
flight_pipeline

extract_flights
extract_airports

transform_flights
transform_airports

combine_data

load_postgres
```

---

# Project 7: Movies and Entertainment Analytics

## Description

Build a pipeline that analyzes movies, actors, genres, and ratings.

This project introduces dimensional modeling concepts that can later evolve into a data warehouse.

## Architecture

```text
TMDB Movies API ──────> Clean Movies ─────┐
                                           ├──> Combine ──> PostgreSQL
TMDB Actors API ──────> Clean Actors ─────┘
```

## Airflow Schedule

Daily.

## Pipeline Steps

1. Extract movies
2. Extract actors
3. Transform movie information
4. Transform actor information
5. Combine datasets
6. Load into PostgreSQL

## Business Questions

* Which genres are trending?
* Which actors appear most frequently?
* Which movies have the highest ratings?

## Suggested DAG

```text
movies_pipeline

extract_movies
extract_actors

transform_movies
transform_actors

combine_data

load_postgres
```

---

# Project 8: Cryptocurrency Sentiment Analysis

## Description

Build a pipeline that combines cryptocurrency prices with social media and technology news.

This project introduces sentiment analysis, enrichment pipelines, and multi-source data integration.

## Architecture

```text
CoinGecko API ───────> Clean Prices ──────┐
                                          │
Reddit API ──────────> Clean Posts ───────┼──> Combine ──> PostgreSQL
                                          │
Hacker News API ─────> Clean News ────────┘
```

## Airflow Schedule

Every 15 minutes.

## Pipeline Steps

1. Extract cryptocurrency prices
2. Extract Reddit discussions
3. Extract Hacker News posts
4. Transform datasets
5. Calculate sentiment scores
6. Combine datasets
7. Load into PostgreSQL

## Business Questions

* What discussions occur before price increases?
* Which words appear most frequently?
* Is there a correlation between sentiment and price movement?

## Suggested DAG

```text
crypto_sentiment_pipeline

extract_crypto
extract_reddit
extract_news

transform_crypto
transform_reddit
transform_news

combine_data

load_postgres
```

---

