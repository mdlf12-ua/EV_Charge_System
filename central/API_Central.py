import os
import logging
from logging.handlers import RotatingFileHandler

from flask import Flask, jsonify, request

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

@app.route("/weather/alert", methods=["POST"])
def weather_alert():
    """
    EV_W notifica una alerta de clima para una localización.
    Body JSON: { "location": "Alicante", "temperature": -3.5 }
    Efecto: marcamos ALERTA_METEO = 1 para los CP de esa localización.
    """
    data = request.get_json(silent=True) or {}
    location = data.get("location")
    temp = data.get("temperature")

    if not location:
        return jsonify({"error": "location es obligatorio"}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO WeatherAlert (location, alert_active, last_temp)
            VALUES (%s, 1, %s)
            ON DUPLICATE KEY UPDATE
                alert_active = 1,
                last_temp = VALUES(last_temp)
        """, (location, temp))

        cursor.execute("""
            UPDATE ChargingPoint
            SET ALERTA_METEO = 1
            WHERE Ubicacion = %s
        """, (location,))


        afectados = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()

        log.info(f"[API_CENTRAL] ALERTA METEO en {location} (T={temp}). CPs afectados: {afectados}")

        return jsonify({
            "status": "alert_registered",
            "location": location,
            "temperature": temp,
            "affected_cps": afectados
        }), 200

    except Exception as e:
        log.error(f"Error en weather_alert para {location}: {e}")
        return jsonify({"error": "Error interno registrando alerta"}), 500


@app.route("/weather/recover", methods=["POST"])
def weather_recover():
    """
    EV_W notifica que se levanta la alerta meteo en una localización.
    Body JSON: { "location": "Alicante", "temperature": 2.0 }
    Efecto: ponemos ALERTA_METEO = 0.
    """
    data = request.get_json(silent=True) or {}
    location = data.get("location")
    temp = data.get("temperature")

    if not location:
        return jsonify({"error": "location es obligatorio"}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO WeatherAlert (location, alert_active, last_temp)
            VALUES (%s, 0, %s)
            ON DUPLICATE KEY UPDATE
                alert_active = 0,
                last_temp = VALUES(last_temp)
        """, (location, temp))

        cursor.execute("""
            UPDATE ChargingPoint
            SET ALERTA_METEO = 0
            WHERE Ubicacion = %s
        """, (location,))


        afectados = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()

        log.info(f"[API_CENTRAL] RECUPERACIÓN METEO en {location} (T={temp}). CPs reactivados: {afectados}")

        return jsonify({
            "status": "recovered",
            "location": location,
            "temperature": temp,
            "affected_cps": afectados
        }), 200

    except Exception as e:
        log.error(f"Error en weather_recover para {location}: {e}")
        return jsonify({"error": "Error interno registrando recuperación"}), 500


if __name__ == "__main__":
    log.info(f"[API_CENTRAL] Iniciando en puerto {REST_PORT}")
    # host=0.0.0.0 para que sea accesible desde fuera del contenedor
    app.run(host="0.0.0.0", port=REST_PORT, debug=False)
