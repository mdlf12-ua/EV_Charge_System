import os
import logging
import secrets
from logging.handlers import RotatingFileHandler
from flask import Flask, jsonify, request
import requests
import urllib3

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, force=True)
log = logging.getLogger("registry")
log.setLevel(logging.INFO)
log.propagate = False

fh = RotatingFileHandler(os.path.join(LOG_DIR, "registry.log"),
                         maxBytes=2_000_000, backupCount=5, encoding="utf-8")
fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
fh.setFormatter(fmt)
log.addHandler(fh)

REGISTRY_PORT = int(os.getenv("REGISTRY_PORT", 9000))
API_CENTRAL_BASE = os.getenv("API_CENTRAL_BASE", "https://192.168.1.33:8000")

app = Flask(__name__)


def generar_credenciales():
    """Genera un token único para autenticación del CP"""
    return secrets.token_hex(32)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "EV_Registry"}), 200


@app.route("/cp/register", methods=["POST"])
def register_cp():
    """
    Alta de un CP en el sistema.
    Body JSON: { "cp_id": "CP001", "ubicacion": "Alicante" }
    
    COMPORTAMIENTO:
    - Si NO existe: crea nuevo con estado REGISTRADO
    - Si existe y registrado=0: lo reactiva (registrado=1, nuevo token)
    - Si existe y registrado=1: RECHAZO (ya está registrado)
    """
    data = request.get_json(silent=True) or {}
    cp_id = data.get("cp_id")
    ubicacion = data.get("ubicacion")
    
    if not cp_id or not ubicacion:
        log.warning("[REGISTRY] Registro sin cp_id o ubicacion")
        return jsonify({"error": "cp_id y ubicacion son obligatorios"}), 400
    
    try:
        # Consultar estado actual
        response = requests.get(
            f"{API_CENTRAL_BASE}/registry/cp/{cp_id}", 
            timeout=5, 
            verify=False
        )
        
        token = generar_credenciales()
        
        if response.status_code == 200:
            # CP existe, verificar si ya está registrado
            cp_data = response.json()
            
            if cp_data.get("registrado") == 1:
                log.warning(f"[REGISTRY] CP {cp_id} YA está registrado")
                return jsonify({
                    "error": "CP ya está registrado. Use /cp/unregister primero si quiere re-registrarlo"
                }), 409  # 409 Conflict
            
            # CP existe pero estaba dado de baja (registrado=0)
            # Lo reactivamos con nuevo token
            log.info(f"[REGISTRY] Reactivando CP {cp_id}")
            update_response = requests.put(
                f"{API_CENTRAL_BASE}/registry/cp/{cp_id}",
                json={
                    "ubicacion": ubicacion, 
                    "token": token, 
                    "registrado": 1,
                    "authenticated": 0  # Debe autenticarse de nuevo
                },
                timeout=5,
                verify=False
            )
            
            if update_response.status_code == 200:
                log.info(f"[REGISTRY] CP {cp_id} reactivado en {ubicacion}")
                return jsonify({
                    "status": "reactivated",
                    "cp_id": cp_id,
                    "ubicacion": ubicacion,
                    "token": token
                }), 200
            else:
                log.error(f"[REGISTRY] Error reactivando: {update_response.text}")
                return jsonify({"error": "Error interno"}), 500
        
        else:
            # CP no existe, crear nuevo
            create_response = requests.post(
                f"{API_CENTRAL_BASE}/registry/cp",
                json={
                    "cp_id": cp_id, 
                    "ubicacion": ubicacion, 
                    "token": token
                },
                timeout=5,
                verify=False
            )
            
            if create_response.status_code in [200, 201]:
                log.info(f"[REGISTRY] CP {cp_id} registrado por primera vez en {ubicacion}")
                return jsonify({
                    "status": "registered",
                    "cp_id": cp_id,
                    "ubicacion": ubicacion,
                    "token": token
                }), 201
            else:
                log.error(f"[REGISTRY] Error creando: {create_response.text}")
                return jsonify({"error": "Error registrando CP"}), 500
        
    except Exception as e:
        log.error(f"[REGISTRY] Excepción registrando {cp_id}: {e}")
        return jsonify({"error": "Error interno"}), 500


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
        response = requests.put(
            f"{API_CENTRAL_BASE}/registry/cp/{cp_id}",
            json={
                "registrado": 0,
                "authenticated": 0,
                "token": None,
                "encryption_key": None
            },
            timeout=5,
            verify=False
        )
        
        if response.status_code == 200:
            log.info(f"[REGISTRY] CP {cp_id} dado de baja")
            return jsonify({
                "status": "unregistered",
                "cp_id": cp_id
            }), 200
        elif response.status_code == 404:
            log.warning(f"[REGISTRY] CP {cp_id} no existe")
            return jsonify({"error": "CP no encontrado"}), 404
        else:
            log.error(f"[REGISTRY] Error en baja: {response.text}")
            return jsonify({"error": "Error interno"}), 500
        
    except Exception as e:
        log.error(f"[REGISTRY] Error dando de baja {cp_id}: {e}")
        return jsonify({"error": "Error interno"}), 500


@app.route("/cp/list", methods=["GET"])
def list_cps():
    """Lista todos los CPs (útil para debug)"""
    try:
        response = requests.get(
            f"{API_CENTRAL_BASE}/registry/cps", 
            timeout=5, 
            verify=False
        )
        
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            log.error(f"[REGISTRY] Error listando: {response.text}")
            return jsonify({"error": "Error interno"}), 500
        
    except Exception as e:
        log.error(f"[REGISTRY] Error listando CPs: {e}")
        return jsonify({"error": "Error interno"}), 500


if __name__ == "__main__":
    log.info(f"[REGISTRY] Iniciando en puerto {REGISTRY_PORT}")
    
    CERT_FILE = "/app/certs/certificado_api_central.crt"
    KEY_FILE  = "/app/certs/clave_privada_api_central.pem"

    log.info(f"[REGISTRY] API_Central: {API_CENTRAL_BASE}")
    app.run(
        host="0.0.0.0", 
        port=REGISTRY_PORT, 
        ssl_context=(CERT_FILE, KEY_FILE), 
        debug=False
    )