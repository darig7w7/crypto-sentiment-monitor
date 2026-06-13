# Ideas de Proyectos con Airflow para Estudiantes de Ingeniería de Datos

## Introducción

Una de las mejores formas de aprender Apache Airflow es construyendo proyectos que consuman datos reales desde APIs, transformen esos datos y los carguen en una base de datos para su análisis.

El objetivo de estos proyectos no es solo aprender Airflow, sino también entender conceptos fundamentales de Ingeniería de Datos como:

* Ingesta de datos
* Transformación de datos
* Enriquecimiento de datos
* Orquestación de datos
* Almacenes de datos
* Programación y monitoreo
* Pipelines ETL y ELT
* Trabajo con APIs externas
* Validación de calidad de datos

Todos los proyectos de este documento siguen una arquitectura similar:

```text
Fuente A ──> Transformar A ──┐
                             ├──> Combinar ──> Data Warehouse
Fuente B ──> Transformar B ──┘
```

Esta estructura ayuda a los estudiantes a entender:

* Tareas paralelas
* Dependencias entre tareas
* Orquestación de DAGs
* Políticas de reintento
* Logging y monitoreo
* Programación de ejecuciones
* Modelado de datos

---

# Proyecto 1: Analítica de Noticias y Criptomonedas

## Descripción

Construir un pipeline que combine precios de criptomonedas con noticias de tecnología y finanzas.

Este proyecto introduce a los estudiantes al trabajo con múltiples APIs y a la correlación de información proveniente de diferentes fuentes.

## Arquitectura

```text
News API ──────────────> Limpiar Noticias ───┐
                                             ├──> Combinar ──> PostgreSQL
CoinGecko API ────────> Limpiar Precios ─────┘
```

## Programación en Airflow

Cada 10 minutos.

## Pasos del Pipeline

1. Extraer artículos de noticias
2. Extraer precios de criptomonedas
3. Limpiar y normalizar noticias
4. Limpiar y normalizar datos de precios
5. Combinar datasets
6. Cargar en PostgreSQL
7. Generar tablas analíticas

## Preguntas de Negocio

* ¿Qué noticias aparecen cuando Bitcoin sube de precio?
* ¿Qué temas están en tendencia?
* ¿Cuántos artículos relacionados con IA aparecen cada día?
* ¿Cómo se comporta Bitcoin durante el día?

## DAG Sugerido

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

# Proyecto 2: Analítica de GitHub

## Descripción

Construir un pipeline que haga seguimiento de repositorios de GitHub y actividad de desarrollo.

Este proyecto es especialmente relevante para estudiantes de ingeniería de software porque ya entienden repositorios, commits y contributors.

## Arquitectura

```text
GitHub Repositories API ──> Limpiar Repos ─────┐
                                                ├──> Combinar ──> PostgreSQL
GitHub Commits API ───────> Limpiar Commits ───┘
```

## Programación en Airflow

Cada hora.

## Pasos del Pipeline

1. Extraer repositorios populares
2. Extraer commits recientes
3. Transformar datos de repositorios
4. Transformar datos de commits
5. Combinar datasets
6. Cargar en PostgreSQL

## Preguntas de Negocio

* ¿Qué repositorios están creciendo más rápido?
* ¿Qué lenguajes de programación son los más populares?
* ¿Cuántos commits ocurren diariamente?
* ¿Qué repositorios reciben más actividad?

## DAG Sugerido

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

# Proyecto 3: Analítica del Mercado Laboral Tecnológico

## Descripción

Construir un pipeline que analice oportunidades laborales remotas y tecnologías en tendencia.

Este proyecto introduce analítica real del mercado laboral.

## Arquitectura

```text
RemoteOK API ──────────> Limpiar Trabajos ─────┐
                                               ├──> Combinar ──> PostgreSQL
GitHub Trending Data ──> Limpiar Skills ───────┘
```

## Programación en Airflow

Cada 6 horas.

## Pasos del Pipeline

1. Extraer ofertas de trabajo
2. Extraer tecnologías en tendencia
3. Normalizar habilidades
4. Relacionar trabajos con tecnologías
5. Cargar en PostgreSQL

## Preguntas de Negocio

* ¿Qué tecnologías tienen mayor demanda?
* ¿Qué trabajos ofrecen los salarios más altos?
* ¿Qué habilidades suelen aparecer juntas?

## DAG Sugerido

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

# Proyecto 4: Monitoreo de Clima y Calidad del Aire

## Descripción

Construir un pipeline que combine información meteorológica con métricas de calidad del aire.

Este proyecto introduce ingeniería de datos ambiental y datasets públicos.

## Arquitectura

```text
Open-Meteo API ───────> Limpiar Clima ───────┐
                                             ├──> Combinar ──> PostgreSQL
OpenAQ API ───────────> Limpiar Aire ────────┘
```

## Programación en Airflow

Cada hora.

## Pasos del Pipeline

1. Extraer datos meteorológicos
2. Extraer datos de calidad del aire
3. Transformar datos meteorológicos
4. Transformar datos de calidad del aire
5. Combinar datasets
6. Cargar en PostgreSQL

## Preguntas de Negocio

* ¿Cuándo es más alta la contaminación?
* ¿Cómo afecta la temperatura a la contaminación?
* ¿Qué ciudades tienen la mejor calidad del aire?

## DAG Sugerido

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

# Proyecto 5: Analítica de la NBA

## Descripción

Construir un pipeline de analítica deportiva usando datos de la NBA.

Este proyecto es atractivo para estudiantes interesados en deportes y demuestra cómo funcionan los sistemas de analítica profesional.

## Arquitectura

```text
NBA Scores API ───────> Limpiar Partidos ───────┐
                                                ├──> Combinar ──> PostgreSQL
NBA Players API ──────> Limpiar Jugadores ──────┘
```

## Programación en Airflow

Diariamente.

## Pasos del Pipeline

1. Extraer resultados de partidos
2. Extraer estadísticas de jugadores
3. Transformar datos de partidos
4. Transformar datos de jugadores
5. Combinar datasets
6. Cargar en PostgreSQL

## Preguntas de Negocio

* ¿Qué jugador anota más puntos?
* ¿Qué equipo tiene la racha de victorias más larga?
* ¿Qué jugadores están mejorando con el tiempo?

## DAG Sugerido

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

# Proyecto 6: Seguimiento de Vuelos en Tiempo Real

## Descripción

Construir un pipeline que haga seguimiento de la actividad aérea alrededor del mundo.

Este proyecto introduce ingesta casi en tiempo real y datasets basados en ubicación.

## Arquitectura

```text
OpenSky API ──────────> Limpiar Vuelos ───────┐
                                             ├──> Combinar ──> PostgreSQL
Airport Database ─────> Limpiar Aeropuertos ─┘
```

## Programación en Airflow

Cada 5 minutos.

## Pasos del Pipeline

1. Extraer información de vuelos
2. Extraer información de aeropuertos
3. Transformar datos de vuelos
4. Transformar datos de aeropuertos
5. Combinar datasets
6. Cargar en PostgreSQL

## Preguntas de Negocio

* ¿Qué rutas tienen más actividad?
* ¿Qué países tienen mayor tráfico aéreo?
* ¿Cuántos vuelos están actualmente sobre Sudamérica?

## DAG Sugerido

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

# Proyecto 7: Analítica de Películas y Entretenimiento

## Descripción

Construir un pipeline que analice películas, actores, géneros y calificaciones.

Este proyecto introduce conceptos de modelado dimensional que luego pueden evolucionar hacia un data warehouse.

## Arquitectura

```text
TMDB Movies API ──────> Limpiar Películas ───┐
                                             ├──> Combinar ──> PostgreSQL
TMDB Actors API ──────> Limpiar Actores ─────┘
```

## Programación en Airflow

Diariamente.

## Pasos del Pipeline

1. Extraer películas
2. Extraer actores
3. Transformar información de películas
4. Transformar información de actores
5. Combinar datasets
6. Cargar en PostgreSQL

## Preguntas de Negocio

* ¿Qué géneros están en tendencia?
* ¿Qué actores aparecen con mayor frecuencia?
* ¿Qué películas tienen las calificaciones más altas?

## DAG Sugerido

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

# Proyecto 8: Análisis de Sentimiento en Criptomonedas

## Descripción

Construir un pipeline que combine precios de criptomonedas con redes sociales y noticias de tecnología.

Este proyecto introduce análisis de sentimiento, pipelines de enriquecimiento e integración de datos de múltiples fuentes.

## Arquitectura

```text
CoinGecko API ───────> Limpiar Precios ──────┐
                                             │
Reddit API ──────────> Limpiar Posts ────────┼──> Combinar ──> PostgreSQL
                                             │
Hacker News API ─────> Limpiar Noticias ─────┘
```

## Programación en Airflow

Cada 15 minutos.

## Pasos del Pipeline

1. Extraer precios de criptomonedas
2. Extraer discusiones de Reddit
3. Extraer posts de Hacker News
4. Transformar datasets
5. Calcular puntajes de sentimiento
6. Combinar datasets
7. Cargar en PostgreSQL

## Preguntas de Negocio

* ¿Qué discusiones ocurren antes de los incrementos de precio?
* ¿Qué palabras aparecen con mayor frecuencia?
* ¿Existe una correlación entre el sentimiento y el movimiento de precios?

## DAG Sugerido

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
