import socket
import threading
import time
import os
import ssl
import sys
import requests
import json
from cryptography.fernet import Fernet
import base64
import hashlib

# === CONFIGURACIÓN ===
HEALTHSTATUS_TIEMPO = 1
REGISTRY_URL = os.getenv("REGISTRY_URL", "https://192.168.1.35:9000")

monitor_state = {
    "cp_id": None,
    "ubicacion": "Barcelona",
    "averiado": False,
    "conocido": False
}

# Estado de autenticación COMPARTIDO con Engine
auth_state = {
    "token": None,
    "encryption_key": None,
    "authenticated": False,
    "cipher": None  # Objeto Fernet para cifrado
}

FORMAT = "utf-8"
HEADER = 64
TLS_ENABLED = os.getenv("TLS_ENABLED", "1") == "1"
TLS_CA = os.getenv("TLS_CA", "/app/certs/certServ.pem")


# === FUNCIONES DE CIFRADO ===
def crear_cipher_desde_key(encryption_key: str):
    """
    Crea un objeto Fernet a partir de la clave hex recibida de Central.
    Fernet requiere una clave de 32 bytes en base64.
    """
    # Convertir hex a bytes y hacer hash para obtener 32 bytes consistentes
    key_bytes = hashlib.sha256(encryption_key.encode()).digest()
    key_b64 = base64.urlsafe_b64encode(key_bytes)
    return Fernet(key_b64)


def cifrar_mensaje(mensaje: str) -> str:
    """Cifra un mensaje usando la clave de sesión"""
    if not auth_state["cipher"]:
        raise Exception("No hay cipher inicializado")
    
    encrypted = auth_state["cipher"].encrypt(mensaje.encode())
    return base64.b64encode(encrypted).decode()


def descifrar_mensaje(mensaje_cifrado: str) -> str:
    """Descifra un mensaje recibido"""
    if not auth_state["cipher"]:
        raise Exception("No hay cipher inicializado")
    
    encrypted_bytes = base64.b64decode(mensaje_cifrado.encode())
    decrypted = auth_state["cipher"].decrypt(encrypted_bytes)
    return decrypted.decode()


# === TLS CONTEXTS ===
def build_tls_client_context_engine() -> ssl.SSLContext:
    cafile = TLS_CA
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cafile)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def build_tls_client_context() -> ssl.SSLContext:
    cafile = os.getenv("TLS_CA", "/app/certs/certServ.pem")
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cafile)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def send_msg(sock: socket.socket, msg: str):
    payload = msg.encode(FORMAT)
    header = str(len(payload)).encode(FORMAT)
    header += b" " * (HEADER - len(header))
    sock.sendall(header)
    sock.sendall(payload)


def recvall(sock: socket.socket, n: int) -> bytes | None:
    data = b""
    while len(data) < n:
        try:
            chunk = sock.recv(n - len(data))
        except (socket.timeout, OSError):
            return None
        if not chunk:
            return None
        data += chunk
    return data


def receive_msg(sock: socket.socket) -> str | None:
    header_bytes = recvall(sock, HEADER)
    if not header_bytes:
        return None
    try:
        length = int(header_bytes.decode(FORMAT).strip())
    except ValueError:
        return None

    body = recvall(sock, length)
    if not body:
        return None
    return body.decode(FORMAT)


# === ENGINE CONNECTOR ===
class EngineConnector():
    def __init__(self, ip, port, cp_id):
        self.ip = ip
        self.puerto = port
        self.id = cp_id
        self.socket = None
        self.thread = None
        self.lock = threading.Lock()
        self.connected = threading.Event()
        self.tls_ctx = build_tls_client_context_engine() if TLS_ENABLED else None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self.try_connect_engine, daemon=True)
        self.thread.start()

    def connect_engine_once(self):
        print(f"[MONITOR] Conectando al Engine ({self.ip}:{self.puerto}){' por TLS' if TLS_ENABLED else ''}...")

        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.settimeout(5)
        raw.connect((self.ip, self.puerto))

        if TLS_ENABLED:
            tls_sock = self.tls_ctx.wrap_socket(raw, server_hostname=self.ip)
            tls_sock.settimeout(5)
            s = tls_sock
        else:
            s = raw

        # Enviar CP_ID y clave de cifrado si está autenticado
        msg_init = f"CP_ID:{self.id}"
        if auth_state["encryption_key"]:
            msg_init += f":{auth_state['encryption_key']}"
        
        send_msg(s, msg_init)
        print(f"[MONITOR] ID {self.id} enviada al Engine.")
        return s

    def try_connect_engine(self):
        while True:
            try:
                socket_temp = self.connect_engine_once()
                print("[MONITOR] Socket Engine Conectado")
                with self.lock:
                    self.socket = socket_temp
                self.connected.set()

                while self.connected.is_set():
                    time.sleep(1)

            except Exception as e:
                self.connected.clear()
                with self.lock:
                    if self.socket:
                        try:
                            self.socket.close()
                        except:
                            pass
                    self.socket = None
                print(f"[MONITOR] No se pudo conectar al engine: {e}. Reintentando en 5s...")
                time.sleep(5)


# === CENTRAL CONNECTOR ===
class CentralConnector():
    def __init__(self, ip, port, cp_id):
        self.ip = ip
        self.puerto = port
        self.id = cp_id
        self.socket = None
        self.thread = None
        self.lock = threading.Lock()
        self.connected = threading.Event()
        self.tls_ctx = build_tls_client_context()

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self.try_connect_central, daemon=True)
        self.thread.start()

    def connect_central_once(self):
        global monitor_state

        print(f"[MONITOR]: Intentando conectar a la Central ({self.ip}:{self.puerto}) por TLS...")

        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.settimeout(5)
        raw.connect((self.ip, self.puerto))
        tls_sock = self.tls_ctx.wrap_socket(raw, server_hostname=self.ip)
        tls_sock.settimeout(5)

        print("[MONITOR] Conectado a Central (TLS)")

        # NO enviar info inicial aquí, esperar a autenticarse primero
        return tls_sock

    def try_connect_central(self):
        while True:
            try:
                socket_temp = self.connect_central_once()
                print("[MONITOR] Socket Central Conectado (TLS)")
                with self.lock:
                    self.socket = socket_temp
                self.connected.set()

                while self.connected.is_set():
                    time.sleep(1)

            except Exception as e:
                self.connected.clear()
                with self.lock:
                    if self.socket:
                        try:
                            self.socket.close()
                        except:
                            pass
                    self.socket = None
                print(f"[MONITOR] No se pudo conectar a central (TLS): {e}. Reintentando en 5s...")
                time.sleep(5)


# === FUNCIONES DE NOTIFICACIÓN (CON CIFRADO) ===
def enviar_mensaje_cifrado_central(central_socket, mensaje_claro, timeout=5):
    """
    Envía un mensaje cifrado a Central usando la clave de sesión.
    Formato: "ENCRYPTED:CP_ID:<base64_mensaje_cifrado>"
    """
    if not auth_state["authenticated"] or not auth_state["cipher"]:
        print("[MONITOR] No autenticado, no se puede cifrar mensaje")
        return False
    
    if not central_socket.connected.wait(timeout):
        return False
    
    with central_socket.lock:
        s = central_socket.socket
    
    if s is None:
        return False
    
    try:
        # Cifrar el mensaje
        mensaje_cifrado = cifrar_mensaje(mensaje_claro)
        # Incluir CP_ID en claro para que Central sepa qué clave usar
        msg_final = f"ENCRYPTED:{monitor_state['cp_id']}:{mensaje_cifrado}"
        
        send_msg(s, msg_final)
        return True
    
    except Exception as e:
        print(f"[MONITOR] Error enviando mensaje cifrado: {e}")
        with central_socket.lock:
            if s is central_socket.socket:
                try: 
                    s.close()
                except: 
                    pass
                central_socket.socket = None
                central_socket.connected.clear()
        return False


def noti_averia(central_socket, motivo, timeout=5):
    """Notifica avería con cifrado"""
    mensaje = f"CP_AVERIA:{monitor_state['cp_id']}:{motivo}"
    return enviar_mensaje_cifrado_central(central_socket, mensaje, timeout)


def noti_recuperacion(central_socket, motivo, timeout=5):
    """Notifica recuperación con cifrado"""
    mensaje = f"CP_RECUPERACION:{monitor_state['cp_id']}:{motivo}"
    return enviar_mensaje_cifrado_central(central_socket, mensaje, timeout)


def enviar_info_a_central(central_socket):
    """
    Envía estado del CP cifrado a Central.
    """
    if not auth_state["authenticated"]:
        print("[MONITOR] Debes autenticarte primero")
        return False
    
    estado_str = "OK" if not monitor_state["averiado"] else "KO"
    msg = f"{monitor_state['cp_id']} {monitor_state['ubicacion']} {estado_str} 0.30"
    
    return enviar_mensaje_cifrado_central(central_socket, msg)


# === FUNCIONES DE REGISTRO Y AUTENTICACIÓN ===
def registrar_en_registry(cp_id, ubicacion):
    """Registra el CP en EV_Registry y obtiene token"""
    try:
        response = requests.post(
            f"{REGISTRY_URL}/cp/register",
            json={"cp_id": cp_id, "ubicacion": ubicacion},
            timeout=10,
            verify=False
        )
        
        if response.status_code in [200, 201]:
            data = response.json()
            token = data.get("token")
            print(f"[MONITOR] ✓ Registrado en Registry exitosamente")
            print(f"[MONITOR] Token recibido: {token[:16]}...")
            return token
        else:
            print(f"[MONITOR] ✗ Error en registro: {response.text}")
            return None
            
    except Exception as e:
        print(f"[MONITOR] ✗ Error conectando a Registry: {e}")
        return None


def autenticar_en_central(central_socket, cp_id, token, timeout=10):
    """
    Autentica el CP en Central usando el token de Registry.
    Retorna la clave de cifrado simétrico.
    """
    if not central_socket.connected.wait(timeout):
        print("[MONITOR] Central no conectada")
        return None
        
    with central_socket.lock:
        s = central_socket.socket
    
    if s is None:
        return None
    
    try:
        # Enviar mensaje de autenticación (SIN cifrar, es el handshake inicial)
        msg = f"CP_AUTH:{cp_id}:{token}"
        send_msg(s, msg)
        print(f"[MONITOR] Enviando autenticación a Central...")
        
        # Recibir respuesta
        respuesta = receive_msg(s)
        
        if respuesta and respuesta.startswith("AUTH_OK:"):
            encryption_key = respuesta.split(":", 1)[1]
            print(f"[MONITOR] ✓ Autenticación exitosa")
            print(f"[MONITOR] Clave de cifrado recibida: {encryption_key[:16]}...")
            
            # Crear cipher para futuras comunicaciones
            auth_state["encryption_key"] = encryption_key
            auth_state["cipher"] = crear_cipher_desde_key(encryption_key)
            auth_state["authenticated"] = True
            
            return encryption_key
        else:
            print(f"[MONITOR] ✗ Autenticación fallida: {respuesta}")
            return None
            
    except Exception as e:
        print(f"[MONITOR] ✗ Error en autenticación: {e}")
        with central_socket.lock:
            if s is central_socket.socket:
                try: 
                    s.close()
                except: 
                    pass
                central_socket.socket = None
                central_socket.connected.clear()
        return None


# === HEALTHCHECK ===
def marcar_engine_caido(engine_socket, s=None):
    with engine_socket.lock:
        if s is None or s == engine_socket.socket:
            try:
                if engine_socket.socket:
                    engine_socket.socket.close()
            except Exception:
                pass
            engine_socket.socket = None
            engine_socket.connected.clear()


def set_averia(monitor_state, central_socket, nuevo_estado, motivo_ok=None, motivo_ko=None):
    estaba_averiado = monitor_state["averiado"]
    conocido = monitor_state["conocido"]

    if nuevo_estado and not estaba_averiado:
        if motivo_ko:
            if noti_averia(central_socket, motivo_ko):
                print("[MONITOR]: Averia notificada a central")
            else:
                print("[MONITOR]: No se pudo conectar a central")
        monitor_state["averiado"] = True
    elif not nuevo_estado and estaba_averiado:
        if motivo_ok:
            if noti_recuperacion(central_socket, motivo_ok):
                print("[MONITOR]: Recuperación notificada a central")
            else:
                print("[MONITOR]: No se pudo conectar a central")
        monitor_state["averiado"] = False
    elif not conocido:
        if noti_recuperacion(central_socket, motivo_ok):
            print("[MONITOR]: Recuperación notificada a central")
        else:
            print("[MONITOR]: No se pudo conectar a central")
        monitor_state["averiado"] = False


def healthstatus_periodico(engine_socket, central_socket):
    global monitor_state
    print("\n[MONITOR] Empezando healthchecks periodicos\n")

    while True:
        if not engine_socket.connected.wait(2):
            continue
        with engine_socket.lock:
            s = engine_socket.socket 
        if s is None:
            continue
        try:
            send_msg(s, "HEALTHSTATUS")
            respuesta = receive_msg(s)
            
            if respuesta is None:
                marcar_engine_caido(engine_socket, s=s)
                set_averia(monitor_state, central_socket, True, motivo_ko="Engine no responde")
                time.sleep(HEALTHSTATUS_TIEMPO)
            elif respuesta == "KO":
                set_averia(monitor_state, central_socket, True, motivo_ko="Engine está KO")
                time.sleep(HEALTHSTATUS_TIEMPO)
            elif respuesta == "OK":
                set_averia(monitor_state, central_socket, False, motivo_ok="Engine está OK")
                monitor_state["conocido"] = True
                time.sleep(HEALTHSTATUS_TIEMPO)

        except ConnectionResetError:
            marcar_engine_caido(engine_socket, s=s)
            set_averia(monitor_state, central_socket, True, motivo_ko="Conexión con Engine perdida")
            time.sleep(HEALTHSTATUS_TIEMPO)
        except Exception as e:
            marcar_engine_caido(engine_socket, s=s)
            set_averia(monitor_state, central_socket, True, motivo_ko="Error en healthcheck")
            time.sleep(HEALTHSTATUS_TIEMPO)


# === MENÚ ===
def menu_monitor(engine_socket, central_socket):
    """Menú interactivo para el CP"""

    while True:
        print("\n============================")
        print("    CP MONITOR (EV_CP_M)    ")
        print("============================")
        print(f" CP: {monitor_state['cp_id']}")
        print(f" Ubicación actual: {monitor_state['ubicacion']}")
        print(f" Averiado: {monitor_state['averiado']}")
        print(f" Autenticado: {'✓' if auth_state['authenticated'] else '✗'}")
        print("============================")
        print(" 1. Registrarse en Registry")
        print(" 2. Autenticarse en Central")
        print(" 3. Cambiar ubicación (ciudad)")
        print(" 4. Reenviar info a Central")
        print(" 0. Salir")
        print("============================")

        op = input(" Elige opción: ").strip()

        if op == "1":
            print("\n--- REGISTRO EN REGISTRY ---")
            ubicacion = input(" Ubicación del CP: ").strip()
            if not ubicacion:
                print(" [MONITOR] Ubicación vacía, usando actual")
                ubicacion = monitor_state["ubicacion"]
            
            token = registrar_en_registry(monitor_state["cp_id"], ubicacion)
            if token:
                auth_state["token"] = token
                monitor_state["ubicacion"] = ubicacion
                print(" [MONITOR] ✓ Registro completado. Ahora puedes autenticarte.")
            
        elif op == "2":
            if not auth_state["token"]:
                print(" [MONITOR] ✗ Primero debes registrarte (opción 1)")
                continue
                
            print("\n--- AUTENTICACIÓN EN CENTRAL ---")
            encryption_key = autenticar_en_central(
                central_socket, 
                monitor_state["cp_id"], 
                auth_state["token"]
            )
            
            if encryption_key:
                print(" [MONITOR] ✓ CP autenticado y listo para operar")
                
                # Notificar a Engine que ya estamos autenticados
                if engine_socket.connected.is_set():
                    with engine_socket.lock:
                        s = engine_socket.socket
                    if s:
                        try:
                            send_msg(s, f"AUTHENTICATED:{encryption_key}")
                            print(" [MONITOR] ✓ Engine notificado de autenticación")
                        except Exception as e:
                            print(f" [MONITOR] Error notificando Engine: {e}")
                
                # Reconectar Engine con la nueva clave para futuras conexiones
                engine_socket.connected.clear()
                with engine_socket.lock:
                    if engine_socket.socket:
                        try:
                            engine_socket.socket.close()
                        except:
                            pass
                    engine_socket.socket = None
            else:
                print(" [MONITOR] ✗ Autenticación fallida")
                auth_state["authenticated"] = False

        elif op == "3":
            nueva = input(" Nueva ciudad: ").strip()
            if not nueva:
                print(" [MONITOR] Ciudad vacía, cancelado.")
                continue

            monitor_state["ubicacion"] = nueva
            print(f"[MONITOR] Ubicación cambiada a: {nueva}")
            enviar_info_a_central(central_socket)

        elif op == "4":
            enviar_info_a_central(central_socket)

        elif op == "0":
            print("[MONITOR] Saliendo...")
            break

        else:
            print(" Opción no válida.")


# === MAIN ===
if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Argumentos incorrectos, el formato es: python EV_CP_M.py <ENGINE_IP> <ENGINE_PORT> <CENTRAL_IP> <CENTRAL_PORT> <CP_ID>\n")
        sys.exit(1)

    engine_ip = sys.argv[1]
    engine_port = int(sys.argv[2])
    central_ip = sys.argv[3]
    central_port = int(sys.argv[4])
    cp_id = sys.argv[5]

    print("[MONITOR]\n")
    print(f"CP ID: {cp_id}\n")
    print(f"Engine: {engine_ip}:{engine_port}\n")
    print(f"Central: {central_ip}:{central_port}\n")
    monitor_state["cp_id"] = cp_id

    engine_socket = EngineConnector(engine_ip, engine_port, cp_id)
    engine_socket.start()

    central_socket = CentralConnector(central_ip, central_port, cp_id)
    central_socket.start()

    t_health = threading.Thread(
        target=healthstatus_periodico,
        args=(engine_socket, central_socket),
        daemon=True
    )
    t_health.start()

    menu_monitor(engine_socket, central_socket)