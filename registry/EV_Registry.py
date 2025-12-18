import os
import logging
import secrets
from logging.handlers import RotatingFileHandler
from flask import Flask, jsonify, request
import requests

# ----------------- LOGGING ----------------- #
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, force=True)
log = logging.getLogger("registry")
log.setLevel(logging.INFO)
log.propagate = False

fh = RotatingFileHandler(os.path.join(LOG_DIR, "registry.log"),
                         maxBytes=2_000_000, backupCount=5, encoding="utf-8")
fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
fh.setFormatter(fmt)
log.addHandler(fh)

# ----------------- CONFIG ----------------- #
REGISTRY_PORT = int(os.getenv("REGISTRY_PORT", 9000))
API_CENTRAL_BASE = os.getenv("API_CENTRAL_BASE", "http://192.168.1.33:8000")

# ----------------- APP FLASK ----------------- #
app = Flask(__name__)


def generar_credenciales():
    """Genera un token único para autenticación del CP"""
    return secrets.token_hex(16)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "EV_Registry"}), 200


@app.route("/cp/register", methods=["POST"])
def register_cp():
    """
    Alta de un CP en el sistema.
    Body JSON: { "cp_id": "CP001", "ubicacion": "Alicante" }
    Devuelve: { "status": "registered", "cp_id": "CP001", "token": "abc123..." }
    """
    data = request.get_json(silent=True) or {}
    cp_id = data.get("cp_id")
    ubicacion = data.get("ubicacion")
    
    if not cp_id:
        log.warning("[REGISTRY] Intento de registro sin cp_id")
        return jsonify({"error": "cp_id es obligatorio"}), 400
    
    if not ubicacion:
        log.warning(f"[REGISTRY] Intento de registro de {cp_id} sin ubicación")
        return jsonify({"error": "ubicacion es obligatoria"}), 400
    
    try:
        # Consultar si ya existe vía API_Central
        response = requests.get(f"{API_CENTRAL_BASE}/registry/cp/{cp_id}", timeout=5)
        
        token = generar_credenciales()
        
        if response.status_code == 200:
            # Ya existe, actualizar
            log.info(f"[REGISTRY] CP {cp_id} ya estaba registrado, actualizando...")
            update_response = requests.put(
                f"{API_CENTRAL_BASE}/registry/cp/{cp_id}",
                json={"ubicacion": ubicacion, "token": token, "registrado": 1},
                timeout=5
            )
            
            if update_response.status_code == 200:
                log.info(f"[REGISTRY] CP {cp_id} actualizado (nuevo token generado)")
                return jsonify({
                    "status": "updated",
                    "cp_id": cp_id,
                    "ubicacion": ubicacion,
                    "token": token
                }), 200
            else:
                log.error(f"[REGISTRY] Error actualizando CP: {update_response.text}")
                return jsonify({"error": "Error actualizando CP"}), 500
        
        else:
            # No existe, crear nuevo
            create_response = requests.post(
                f"{API_CENTRAL_BASE}/registry/cp",
                json={"cp_id": cp_id, "ubicacion": ubicacion, "token": token, "registrado": 1},
                timeout=5
            )
            
            if create_response.status_code in [200, 201]:
                log.info(f"[REGISTRY] CP {cp_id} registrado exitosamente en {ubicacion}")
                return jsonify({
                    "status": "registered",
                    "cp_id": cp_id,
                    "ubicacion": ubicacion,
                    "token": token
                }), 201
            else:
                log.error(f"[REGISTRY] Error creando CP: {create_response.text}")
                return jsonify({"error": "Error registrando CP"}), 500
        
    except Exception as e:
        log.error(f"[REGISTRY] Error registrando CP {cp_id}: {e}")
        return jsonify({"error": "Error interno registrando CP"}), 500


@app.route("/cp/unregister", methods=["DELETE"])
def unregister_cp():
    """
    Baja de un CP en el sistema.
    Body JSON: { "cp_id": "CP001" }
    """
    data = request.get_json(silent=True) or {}
    cp_id = data.get("cp_id")
    
    if not cp_id:
        return jsonify({"error": "cp_id es obligatorio"}), 400
    
    try:
        # Marcar como no registrado vía API_Central
        response = requests.put(
            f"{API_CENTRAL_BASE}/registry/cp/{cp_id}",
            json={"registrado": 0},
            timeout=5
        )
        
        if response.status_code == 200:
            log.info(f"[REGISTRY] CP {cp_id} dado de baja exitosamente")
            return jsonify({
                "status": "unregistered",
                "cp_id": cp_id
            }), 200
        elif response.status_code == 404:
            log.warning(f"[REGISTRY] Intento de dar de baja CP inexistente: {cp_id}")
            return jsonify({"error": "CP no encontrado"}), 404
        else:
            log.error(f"[REGISTRY] Error en baja: {response.text}")
            return jsonify({"error": "Error interno"}), 500
        
    except Exception as e:
        log.error(f"[REGISTRY] Error dando de baja CP {cp_id}: {e}")
        return jsonify({"error": "Error interno"}), 500


@app.route("/cp/authenticate", methods=["POST"])
def authenticate_cp():
    """
    Autenticación de un CP (valida token).
    Body JSON: { "cp_id": "CP001", "token": "abc123..." }
    Devuelve: { "authenticated": true/false }
    """
    data = request.get_json(silent=True) or {}
    cp_id = data.get("cp_id")
    token = data.get("token")
    
    if not cp_id or not token:
        return jsonify({"error": "cp_id y token son obligatorios"}), 400
    
    try:
        # Validar vía API_Central
        response = requests.post(
            f"{API_CENTRAL_BASE}/registry/authenticate",
            json={"cp_id": cp_id, "token": token},
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("authenticated"):
                log.info(f"[REGISTRY] Autenticación exitosa para CP {cp_id}")
                return jsonify({
                    "authenticated": True,
                    "cp_id": cp_id,
                    "ubicacion": data.get("ubicacion")
                }), 200
            else:
                log.warning(f"[REGISTRY] Autenticación fallida para CP {cp_id}")
                return jsonify({
                    "authenticated": False,
                    "error": "Credenciales inválidas"
                }), 401
        else:
            log.warning(f"[REGISTRY] Autenticación fallida para CP {cp_id}")
            return jsonify({
                "authenticated": False,
                "error": "Credenciales inválidas o CP no registrado"
            }), 401
            
    except Exception as e:
        log.error(f"[REGISTRY] Error autenticando CP {cp_id}: {e}")
        return jsonify({"error": "Error interno"}), 500


@app.route("/cp/list", methods=["GET"])
def list_cps():
    """Lista todos los CPs registrados (útil para debug)"""
    try:
        response = requests.get(f"{API_CENTRAL_BASE}/registry/cps", timeout=5)
        
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            log.error(f"[REGISTRY] Error listando CPs: {response.text}")
            return jsonify({"error": "Error interno"}), 500
        
    except Exception as e:
        log.error(f"[REGISTRY] Error listando CPs: {e}")
        return jsonify({"error": "Error interno"}), 500


if __name__ == "__main__":
    log.info(f"[REGISTRY] Iniciando en puerto {REGISTRY_PORT}")
    log.info(f"[REGISTRY] API_Central: {API_CENTRAL_BASE}")
    app.run(host="0.0.0.0", port=REGISTRY_PORT, debug=False)