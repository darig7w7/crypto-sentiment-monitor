
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
    cur.execute("""
        SELECT fetched_at, source, title, compound, type
        FROM sentiment_items
        ORDER BY fetched_at DESC
        LIMIT 30
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([
        {
            "fetched_at": str(r[0]),
            "source": r[1],
            "title": r[2],
            "compound": float(r[3]),
            "type": r[4],
        }
        for r in rows
    ])

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
if __name__ == "__main__":
    app.run(debug=True, port=5000)
    