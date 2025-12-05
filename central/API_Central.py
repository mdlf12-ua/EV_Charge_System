import os
import logging
from logging.handlers import RotatingFileHandler

from flask import Flask, jsonify
import mysql.connector

# ----------------- LOGGING BÁSICO ----------------- #

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, force=True)
log = logging.getLogger("api_central")
log.setLevel(logging.INFO)
log.propagate = False

fh = RotatingFileHandler(os.path.join(LOG_DIR, "api_central.log"),
                         maxBytes=2_000_000, backupCount=5, encoding="utf-8")
fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
fh.setFormatter(fmt)
log.addHandler(fh)

# ----------------- CONFIG ----------------- #

REST_PORT = int(os.getenv("REST_PORT", 8000))

DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "usuario")
DB_PASSWORD = os.getenv("DB_PASSWORD", "contraseña")
DB_NAME = os.getenv("DB_NAME", "database")

# ----------------- APP FLASK ----------------- #

app = Flask(__name__)


def get_db_connection():
    """
    Abre una conexión nueva a MySQL.
    (Simple para empezar, sin pool ni nada raro.)
    """
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    return conn


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "API_Central"}), 200


@app.route("/cps", methods=["GET"])
def get_cps():
    """
    Devuelve todos los CPs desde la tabla ChargingPoint.
    De momento, solo para probar que el API funciona.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM ChargingPoint")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify(rows), 200

    except Exception as e:
        log.error(f"Error consultando CPs: {e}")
        return jsonify({"error": "Error interno consultando CPs"}), 500


if __name__ == "__main__":
    log.info(f"[API_CENTRAL] Iniciando en puerto {REST_PORT}")
    # host=0.0.0.0 para que sea accesible desde fuera del contenedor
    app.run(host="0.0.0.0", port=REST_PORT, debug=False)
