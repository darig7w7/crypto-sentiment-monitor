# Crypto Sentiment Monitor

Pipeline de datos que recopila precios de criptomonedas y analiza el sentimiento
de noticias y publicaciones en tiempo real, correlacionando ambas señales cada hora.

## Introducción

El sentimiento promedio de noticias y publicaciones sobre criptomonedas
está correlacionado con el movimiento del precio en la siguiente hora.
Si el mercado reacciona a la narrativa mediática, debería existir una
correlación positiva entre un sentimiento alto y una subida de precio,
y viceversa. Este pipeline permite observar esa relación con datos reales.

## Arquitectura
CoinGecko API ──────────────────────────────────┐

▼

NewsAPI ──────── filtro cripto ──── VADER ──── PostgreSQL ──── Flask API ──── Dashboard

▲

HackerNews ──────────────────────────────────────┘

Orquestado con Apache Airflow. Cada hora el DAG `crypto_sentiment` ejecuta
las tres extracciones en paralelo, analiza el sentimiento y persiste los resultados.

## Conceptos clave

### ¿Qué es un pipeline ETL?

ETL significa Extract, Transform, Load. Es el patrón base de ingeniería de datos:

- **Extract** — obtener datos crudos de fuentes externas (APIs, bases de datos, archivos)
- **Transform** — limpiar, filtrar y procesar esos datos para que sean útiles
- **Load** — persistir el resultado en un almacén de datos para análisis posterior

En este proyecto cada fase tiene una responsabilidad clara:

| Fase | Tasks |
|---|---|
| Extract | `fetch_prices`, `fetch_news`, `fetch_hackernews` |
| Transform | `analyze_sentiment` |
| Load | `save_to_postgres` |

### ¿Qué es Apache Airflow?

Airflow es un orquestador de pipelines de datos. Su función es decidir cuándo
correr cada tarea, en qué orden, con qué dependencias, y qué hacer si algo falla.

Sin Airflow, un script Python que llama APIs tiene varios problemas:
- No se ejecuta solo cada hora
- Si una API falla, todo el proceso se pierde
- No hay visibilidad de qué corrió bien y qué no

Airflow resuelve los tres con reintentos automáticos, scheduling y una
interfaz web para monitorear cada ejecución.

### ¿Qué es un DAG?

DAG significa Directed Acyclic Graph — grafo dirigido acíclico. En Airflow
cada pipeline se define como un DAG: un archivo Python que describe las tareas
y sus dependencias. "Acíclico" significa que no hay ciclos — las tareas solo
avanzan, nunca vuelven atrás.

Las dependencias permiten paralelismo. En este DAG las tres extracciones
no dependen entre sí, por lo que Airflow las corre simultáneamente:

fetch_prices ─────────────────────────┐

▼

fetch_news ──── analyze_sentiment ──── save_to_postgres

▲

fetch_hackernews ─────────────────────┘

Esto reduce el tiempo total de ejecución de ~18 segundos a ~6 segundos.

### ¿Qué es VADER?

VADER (Valence Aware Dictionary and sEntiment Reasoner) es un modelo de
análisis de sentimiento diseñado específicamente para texto de internet.
A diferencia de modelos de machine learning que requieren entrenamiento,
VADER usa un diccionario de palabras con pesos predefinidos.

El resultado principal es el score `compound`, un valor entre -1 y +1:

| Rango | Interpretación |
|---|---|
| > 0.05 | Positivo |
| -0.05 a 0.05 | Neutral |
| < -0.05 | Negativo |

VADER funciona bien con titulares de noticias porque está calibrado para
texto corto, informal y con carga emocional.

### ¿Por qué filtrar las noticias?

NewsAPI devuelve artículos de fuentes como Crypto Briefing que mezclan
contenido cripto con noticias generales. Sin filtro, titulares del Mundial
o de política contaminan el análisis de sentimiento. El filtro verifica
que cada artículo contenga al menos una keyword del dominio cripto
antes de incluirlo en el análisis.

## Tecnologías

| Capa | Tecnología |
|---|---|
| Orquestación | Apache Airflow 2.10 |
| Análisis de sentimiento | VADER (vaderSentiment) |
| Base de datos | PostgreSQL 16 |
| Backend | Flask 3.1 |
| Infraestructura | Docker + Docker Compose |

## Fuentes de datos

- **CoinGecko** — precios y variación 24h de Bitcoin y Ethereum (sin API key)
- **NewsAPI** — artículos de medios especializados filtrados por keywords cripto
- **HackerNews** — posts de la comunidad tech vía Algolia API (sin API key)

## Requisitos

- Docker y Docker Compose
- Python 3.10+
- API key de NewsAPI (gratuita en newsapi.org)

## Instalación

### 1. Clonar el repositorio

```bash
git clone <url-del-repo>
cd data_engineering_course_lab4
```

### 2. Configurar variables de entorno

```bash
cp tmdb-movie-metadata/.env.example tmdb-movie-metadata/.env
```

Editar `airflow/docker-compose.yml` y reemplazar la NEWS_API_KEY:

```yaml
NEWS_API_KEY: ${NEWS_API_KEY:-tu_key_aqui}
```

### 3. Levantar Airflow

```bash
cd airflow
sudo chown -R 50000:0 logs/
docker compose up -d
```

El webserver estará disponible en el puerto `8081` (no 8080).
Airflow tarda 2-3 minutos en iniciar completamente.

Usuario: `admin` / Contraseña: `admin`

### 4. Crear las tablas en PostgreSQL

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
    title TEXT,
    compound NUMERIC,
    type VARCHAR(20)
);
```

### 5. Activar el DAG

En la interfaz de Airflow activar `crypto_sentiment` y ejecutarlo manualmente
con el botón Trigger DAG. A partir de ese momento corre automáticamente cada hora.

### 6. Levantar el dashboard

```bash
# Si no tienes python3-venv instalado
sudo apt install python3-venv python3-full -y

cd dashboard
python3 -m venv venv
source venv/bin/activate
pip install flask flask-cors psycopg2-binary
python app.py
```

Dashboard disponible en `http://127.0.0.1:5000`

## Estructura del proyecto

data_engineering_course_lab4/

├── airflow/

│   ├── dags/

│   │   └── crypto_sentiment.py   # DAG principal

│   └── docker-compose.yml

└── dashboard/

├── app.py                    # API Flask

└── index.html                # Dashboard

## Esquema de base de datos

crypto_prices          crypto_sentiment       sentiment_items

─────────────          ────────────────       ───────────────

id                     id                     id

fetched_at             fetched_at             fetched_at

coin                   average_sentiment      source

price_usd              label                  title

change_24h             total_analyzed         compound

type

## Consulta de correlación

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

## Decisiones de diseño

### Por qué no se usó Reddit

Reddit fue considerado como fuente inicial dado el volumen de discusión
sobre criptomonedas en subreddits como r/CryptoCurrency. Sin embargo,
desde 2023 Reddit restringe el acceso a su API a cuentas verificadas
con historial de actividad, rechazando aplicaciones de cuentas nuevas.

Como alternativa se optó por HackerNews, cuya API es pública, estable
y sin restricciones. La comunidad de HackerNews produce contenido más
analítico sobre cripto que Reddit, lo que mejora la calidad del
análisis de sentimiento con VADER, un modelo calibrado para texto
técnico e informativo más que para lenguaje emocional o de jerga.


## Preguntas de Negocio
¿Qué discusiones ocurren antes de los incrementos de precio?
¿Qué palabras aparecen con mayor frecuencia?
¿Existe una correlación entre el sentimiento y el movimiento de precios?
