
from flask import Flask, jsonify, send_file
from flask_cors import CORS
import psycopg2

app = Flask(__name__)
CORS(app)

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        dbname="airflow",
        user="airflow",
        password="airflow",
        port=5432,
    )

@app.route("/api/prices")
def get_prices():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT fetched_at, coin, price_usd, change_24h
        FROM crypto_prices
        ORDER BY fetched_at DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([
        {
            "fetched_at": str(r[0]),
            "coin": r[1],
            "price_usd": float(r[2]),
            "change_24h": float(r[3]),
        }
        for r in rows
    ])

@app.route("/api/sentiment")
def get_sentiment():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT fetched_at, average_sentiment, label, total_analyzed
        FROM crypto_sentiment
        ORDER BY fetched_at DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([
        {
            "fetched_at": str(r[0]),
            "average_sentiment": float(r[1]),
            "label": r[2],
            "total_analyzed": r[3],
        }
        for r in rows
    ])

@app.route("/api/items")
def get_items():
    conn = get_db_connection()
    cur = conn.cursor()
    # Le agregamos un WHERE para que solo traiga títulos que hablen de criptos reales
    cur.execute(
        """
        SELECT fetched_at, source, title, compound, type
        FROM sentiment_items
        WHERE (
            lower(title) LIKE '%bitcoin%' OR 
            lower(title) LIKE '%btc%' OR 
            lower(title) LIKE '%ethereum%' OR 
            lower(title) LIKE '%eth%' OR 
            lower(title) LIKE '%blockchain%' OR 
            lower(title) LIKE '%coinbase%'
        )
        ORDER BY fetched_at DESC
        LIMIT 30
    """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(
        [
            {
                "fetched_at": str(r[0]),
                "source": r[1],
                "title": r[2],
                "compound": float(r[3]) if r[3] is not None else 0.0,
                "type": r[4],
            }
            for r in rows
        ]
    )
@app.route("/api/correlation")
def get_correlation():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
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
        ORDER BY cs.fetched_at ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([
        {
            "fetched_at": str(r[0]),
            "average_sentiment": float(r[1]),
            "label": r[2],
            "coin": r[3],
            "price_usd": float(r[4]),
            "change_24h": float(r[5]),
        }
        for r in rows
    ])
@app.route("/")
def index():
    return send_file("index.html")

@app.route("/api/words")
def get_words():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT word, COUNT(*) as frecuencia
        FROM (
            SELECT regexp_split_to_table(lower(title), '\\s+') as word
            FROM sentiment_items
        ) words
        WHERE length(word) > 4
        AND word NOT IN ('with','that','this','from','have','been','will',
            'their','about','would','could','after','bitcoin','ethereum',
            'crypto','which','there','other','more','also','when','than')
        GROUP BY word
        ORDER BY frecuencia DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([{"word": r[0], "count": r[1]} for r in rows])
@app.route("/api/top_sources")
def top_sources():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            source,
            ROUND(AVG(compound)::numeric,4) as avg_sentiment,
            COUNT(*) as total
        FROM sentiment_items
        GROUP BY source
        ORDER BY avg_sentiment DESC
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "source": r[0],
            "avg_sentiment": float(r[1]),
            "total": r[2]
        }
        for r in rows
    ])


@app.route("/api/btc_stats")
def btc_stats():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            MAX(price_usd),
            MIN(price_usd),
            AVG(price_usd)
        FROM crypto_prices
        WHERE coin='bitcoin'
    """)

    row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify({
        "max": float(row[0]),
        "min": float(row[1]),
        "avg": float(row[2])
    })


@app.route("/api/sentiment_distribution")
def sentiment_distribution():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            CASE
                WHEN compound > 0.05 THEN 'positive'
                WHEN compound < -0.05 THEN 'negative'
                ELSE 'neutral'
            END AS sentiment,
            COUNT(*)
        FROM sentiment_items
        GROUP BY sentiment
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "sentiment": r[0],
            "count": r[1]
        }
        for r in rows
    ])
    
@app.route("/api/price_movement")
def get_price_movement():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            cp1.fetched_at as momento,
            cp1.price_usd as precio_actual,
            cp2.price_usd as precio_siguiente,
            cp2.price_usd - cp1.price_usd as variacion,
            cs.average_sentiment,
            cs.label
        FROM crypto_prices cp1
        JOIN crypto_prices cp2 
            ON cp1.coin = cp2.coin
            AND cp2.fetched_at = (
                SELECT MIN(fetched_at) 
                FROM crypto_prices 
                WHERE fetched_at > cp1.fetched_at 
                AND coin = cp1.coin
            )
        JOIN crypto_sentiment cs 
            ON DATE_TRUNC('minute', cs.fetched_at) = DATE_TRUNC('minute', cp1.fetched_at)
        WHERE cp1.coin = 'bitcoin'
        ORDER BY cp1.fetched_at ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([
        {
            "momento": str(r[0]),
            "precio_actual": float(r[1]),
            "precio_siguiente": float(r[2]),
            "variacion": float(r[3]),
            "sentiment": float(r[4]),
            "label": r[5],
        }
        for r in rows
    ])
if __name__ == "__main__":
    app.run(debug=True, port=5000)
    