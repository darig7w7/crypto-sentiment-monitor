# Crypto Sentiment Pipeline

Proyecto de Ingeniería de Datos (Proyecto 8) que combina precios de criptomonedas con análisis de sentimiento de noticias y posts tecnológicos, usando Docker, Airflow, CoinGecko, NewsAPI, Hacker News, VADER y PostgreSQL.

El pipeline extrae datos de tres fuentes cada 15 minutos y los correlaciona:

```text
CoinGecko API ───────> transform_crypto ──────┐
                                              │
NewsAPI (alt. Reddit) ──> transform_news ─────┼──> combine_data ──> PostgreSQL
                                              │
Hacker News API ────────> transform_news ─────┘
```

## Estructura del proyecto

```text
airflow/
```

Entorno de Airflow con el DAG `crypto_sentiment_pipeline`. Corre cada 15 minutos, extrae precios y texto, calcula puntajes de sentimiento y carga los resultados en PostgreSQL.

```text
dashboard/
```

API Flask y dashboard HTML que lee de PostgreSQL y visualiza precios, sentimiento en el tiempo, frecuencia de palabras y correlación con movimiento de precios.

```text
project-idea.md
project-idea.es.md
```

Documento de idea del proyecto en inglés y español.

## Requisitos

- Docker y Docker Compose
- Python 3.10 o superior
- API key de NewsAPI (gratuita en newsapi.org)

## Decisión de diseño: NewsAPI en lugar de Reddit

La API de Reddit rechaza credenciales de cuentas nuevas desde 2023. NewsAPI provee artículos de medios especializados en cripto como CoinDesk y Cointelegraph. La task `extract_reddit` se conserva en el DAG para mantener la arquitectura del proyecto, con una nota documentada que explica la sustitución.

## 1. Configurar el entorno

Crear el archivo de variables de entorno:

```bash
cp tmdb-movie-metadata/.env.example tmdb-movie-metadata/.env
```

Editar `tmdb-movie-metadata/.env` y agregar la clave de NewsAPI al final del archivo:

```env
NEWS_API_KEY=tu_clave_aqui
```

> Este archivo es cargado por Airflow como fuente de variables de entorno.
> La carpeta `tmdb-movie-metadata` es parte de la estructura base del laboratorio.
## 2. Levantar Airflow

```bash
cd airflow
sudo chown -R 50000:0 logs/
docker compose up -d
```

Airflow estará disponible en:

```text
http://localhost:8081
```

Credenciales:

```text
admin / admin
```

Nota: Airflow tarda 2-3 minutos en iniciar por primera vez. El webserver corre en el puerto `8081`, no `8080`.

## 3. Crear las tablas en PostgreSQL

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

## 4. Ejecutar el DAG

En la interfaz de Airflow:

1. Abrir `crypto_sentiment_pipeline`
2. Activar el DAG con el toggle
3. Dispararlo manualmente con el botón Trigger DAG
4. Revisar los logs de cada task

Flujo de tasks:

```text
extract_crypto ─────────────────────────────────────────┐
extract_reddit ─────────────────────────────────────────┤
extract_news ───┐                                       │
                ├──> transform_news ──> combine_data ───┼──> load_postgres
extract_hackernews ─┘                                   │
                                        transform_crypto─┘
```

Pasos del pipeline:

1. Extraer precios de criptomonedas
2. Extraer discusiones de Reddit (conservado, cubierto por NewsAPI)
3. Extraer posts de Hacker News
4. Transformar datasets
5. Calcular puntajes de sentimiento
6. Combinar datasets
7. Cargar en PostgreSQL

El DAG corre automáticamente cada 15 minutos. El resultado final se imprime en los logs de `load_postgres`:

```json
{
  "fetched_at": "2026-06-14 02:00:07",
  "bitcoin_price": 64600,
  "average_sentiment": 0.1541,
  "total_analyzed": 15
}
```

## 5. Levantar el dashboard

```bash
sudo apt install python3-venv python3-full -y

cd dashboard
python3 -m venv venv
source venv/bin/activate
pip install flask flask-cors psycopg2-binary
python app.py
```

Dashboard disponible en:

```text
http://127.0.0.1:5000
```

## Preguntas de negocio

**¿Qué discusiones ocurren antes de los incrementos de precio?**

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

**¿Qué palabras aparecen con mayor frecuencia?**

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

**¿Existe correlación entre el sentimiento y el movimiento de precios?**

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

## Detener todo

```bash
cd airflow
docker compose down
```

## Solución de problemas

Si Airflow no aparece en `8081`:

```bash
cd airflow
docker compose ps
docker compose logs --tail=50 airflow-webserver
```

Si el webserver falla con errores de permisos en logs:

```bash
cd airflow
sudo chown -R 50000:0 logs/
docker compose down -v
docker compose up -d
```

Si PostgreSQL no es accesible desde el dashboard, verificar que el puerto esté expuesto:

```bash
docker compose ps
# postgres debe mostrar 0.0.0.0:5432->5432/tcp
```

Si el DAG muestra errores de importación, verificar que `vaderSentiment` esté en `_PIP_ADDITIONAL_REQUIREMENTS` dentro de `docker-compose.yml`:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: requests==2.31.0 psycopg2-binary==2.9.9 vaderSentiment==3.3.2
```
