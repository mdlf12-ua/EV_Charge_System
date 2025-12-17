import socket
import threading
import time
import os
import ssl
import sys

HEALTHSTATUS_TIEMPO = 1

monitor_state = {
    "cp_id": None,
    "ubicacion": "Barcelona",
    "averiado": False,
    "conocido": False
}


FORMAT = "utf-8"
HEADER = 64

TLS_ENABLED = os.getenv("TLS_ENABLED", "1") == "1"
TLS_CA = os.getenv("TLS_CA", "/app/certs/certServ.pem")

def build_tls_client_context_engine() -> ssl.SSLContext:
    cafile = TLS_CA
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cafile)
    ctx.check_hostname = False          # en docker/IP normalmente no cuadra CN
    ctx.verify_mode = ssl.CERT_REQUIRED # valida cert del servidor (self-signed OK si está en cafile)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def send_msg(sock: socket.socket, msg: str):
    payload = msg.encode(FORMAT)
    header = str(len(payload)).encode(FORMAT)
    header += b" " * (HEADER - len(header))
    sock.sendall(header)
    sock.sendall(payload)


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
            # SNI: usamos el hostname del servicio docker (engine1, engine2, etc.)
            tls_sock = self.tls_ctx.wrap_socket(raw, server_hostname=self.ip)
            tls_sock.settimeout(5)
            s = tls_sock
        else:
            s = raw

        send_msg(s, f"CP_ID:{self.id}")
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

TLS_CERT = os.getenv("TLS_CERT", "/app/certs/certServ.pem")

def build_tls_client_context() -> ssl.SSLContext:
    cafile = os.getenv("TLS_CA", "/app/certs/certServ.pem")

    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cafile)
    ctx.check_hostname = False          # simplifica (en docker/ip no cuadra CN)
    ctx.verify_mode = ssl.CERT_REQUIRED # valida que es "el server" correcto

    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx

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

        estado_str = "OK" if not monitor_state["averiado"] else "KO"
        msg_inicial = f"{self.id} {monitor_state['ubicacion']} {estado_str} 0.30"
        send_msg(tls_sock, msg_inicial)
        print(f"[MONITOR] Información inicial enviada: {msg_inicial}")

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


def noti_averia(central_socket, motivo, timeout=5):
    if not central_socket.connected.wait(timeout):
        return False
    with central_socket.lock:
        s = central_socket.socket
    if s is None:
        return False
    try:
        msg= f"CP_AVERIA:{monitor_state['cp_id']}:{motivo}"
        send_msg(s, msg)
        return True
    
    except Exception as e:
        with central_socket.lock:
            if s is central_socket.socket:
                try: s.close()
                except: pass
                central_socket.socket = None
                central_socket.connected.clear()
        return False

def noti_recuperacion(central_socket, motivo, timeout=5):
    if not central_socket.connected.wait(timeout):
        return False
    with central_socket.lock:
        s = central_socket.socket
    if s is None:
        return False
    try:
        msg= f"CP_RECUPERACION:{monitor_state['cp_id']}:{motivo}"
        send_msg(s, msg)
        return True
    
    except Exception as e:
        with central_socket.lock:
            if s is central_socket.socket:
                try: s.close()
                except: pass
                central_socket.socket = None
                central_socket.connected.clear()
        return False

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
            #print("Mensaje mandado a engine")
            respuesta=receive_msg(s)
            #print("Health")
            if respuesta is None:
                #print("Sin respuesta")                
                marcar_engine_caido(engine_socket, s=s)
                set_averia(monitor_state, central_socket, True, motivo_ko="Engine no responde")
                time.sleep(HEALTHSTATUS_TIEMPO)

            elif respuesta == "KO":
                #print("KO")   
                set_averia(monitor_state, central_socket, True, motivo_ko="Engine está KO")
                time.sleep(HEALTHSTATUS_TIEMPO)

            elif respuesta == "OK":
                #print("OK")
                set_averia(monitor_state, central_socket, False, motivo_ok="Engine está OK")
                monitor_state["conocido"] = True
                time.sleep(HEALTHSTATUS_TIEMPO)

        except ConnectionResetError:
            #print("Excepcion Connection")
            marcar_engine_caido(engine_socket, s=s)
            set_averia(monitor_state, central_socket, True, motivo_ko="Conexión con Engine perdida")
            # no hagas break; deja que el bucle espere a reconexión
            time.sleep(HEALTHSTATUS_TIEMPO)

        except Exception as e:
            #print("Excepcion")
            marcar_engine_caido(engine_socket, s=s)
            # aquí no hace falta re-notificar si ya estabas en KO; set_averia se encarga
            set_averia(monitor_state, central_socket, True, motivo_ko="Error en healthcheck")
            time.sleep(HEALTHSTATUS_TIEMPO)
def enviar_info_a_central(central_socket):
    """
    Reenvía a Central el mensaje estándar:
    "<cp_id> <ubicacion> <OK/KO> <precio>"
    Central ya lo parsea así.
    """
    if not central_socket.connected.wait(3):
        print("[MONITOR] Central no conectada todavía.")
        return False

    with central_socket.lock:
        s = central_socket.socket

    if s is None:
        print("[MONITOR] Central socket es None.")
        return False

    estado_str = "OK" if not monitor_state["averiado"] else "KO"
    msg = f"{monitor_state['cp_id']} {monitor_state['ubicacion']} {estado_str} 0.30"

    try:
        send_msg(s, msg)
        print(f"[MONITOR] Info enviada a Central: {msg}")
        return True
    except Exception as e:
        print(f"[MONITOR] Error enviando info a Central: {e}")
        return False


def menu_monitor(engine_socket, central_socket):
    """
    Menú interactivo para pruebas (cambiar ubicación).
    """
    while True:
        print("\n============================")
        print("    CP MONITOR (EV_CP_M)    ")
        print("============================")
        print(f" CP: {monitor_state['cp_id']}")
        print(f" Ubicación actual: {monitor_state['ubicacion']}")
        print(f" Averiado: {monitor_state['averiado']}")
        print("============================")
        print(" 1. Cambiar ubicación (ciudad)")
        print(" 2. Reenviar info a Central")
        print(" 0. Salir")
        print("============================")

        op = input(" Elige opción: ").strip()

        if op == "1":
            nueva = input(" Nueva ciudad: ").strip()
            if not nueva:
                print(" [MONITOR] Ciudad vacía, cancelado.")
                continue

            monitor_state["ubicacion"] = nueva
            print(f"[MONITOR] Ubicación cambiada a: {nueva}")

            # reenvía para que Central “vea” el cambio
            enviar_info_a_central(central_socket)

        elif op == "2":
            enviar_info_a_central(central_socket)

        elif op == "0":
            print("[MONITOR] Saliendo...")
            break

        else:
            print(" Opción no válida.")

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

    # Healthchecks en background
    t_health = threading.Thread(
        target=healthstatus_periodico,
        args=(engine_socket, central_socket),
        daemon=True
    )
    t_health.start()

    # Menú en primer plano (para poder usar input)
    menu_monitor(engine_socket, central_socket)
