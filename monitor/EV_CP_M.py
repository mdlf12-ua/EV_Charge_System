import socket
import threading
import time
import sys

FORMAT = 'utf-8'
HEADER = 64
FIN = "FIN"
HEALTHSTATUS_TIEMPO = 1 #(segundos)

monitor_state = {
    "cp_id": None,
    "ubicacion": "Barcelona",
    "averiado": False
}
class EngineConnector():
    def __init__(self,ip,port,cp_id):
        self.ip = ip
        self.puerto = port
        self.id = cp_id
        self.socket = None
        self.thread = None
        self.lock = threading.Lock()
        self.connected = threading.Event()

    def start(self):
        if self.thread and self.thread.is_alive():
                    return
        self.thread = threading.Thread(target=self.try_connect_engine, daemon=True)
        self.thread.start()
    def connect_engine_once(self):
        print(f"[MONITOR] Conectando al Engine ({self.ip}:{self.puerto})...")
        engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_socket.connect((engine_ip, engine_port))

        send_msg(engine_socket, f"CP_ID:{cp_id}")
        print(f"[MONITOR] ID {cp_id} enviada al Engine.")
        return engine_socket
    def try_connect_engine(self):
        while True:
            try:
                socket_temp = self.connect_engine_once()
                print("Socket Conectado")
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

def send_msg(sock, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    sock.sendall(send_length)
    sock.sendall(message)


def receive_msg(socket):
    try:
        length = int(socket.recv(HEADER).decode(FORMAT).strip())
        return socket.recv(length).decode(FORMAT)
    except:
        return None


def noti_averia(central_socket, motivo):
    if not central_socket:
        print("\n[MONITOR] No hay conexión con Central")
        return
    try:
        msg= f"CP_AVERIA:{monitor_state['cp_id']}:{motivo}"
        send_msg(central_socket, msg)
        print("\n[MONITOR] Averia notificada a Central")
        response = receive_msg(central_socket)
    
    except Exception as e:
        print("\n[MONITOR] Error en la notificación de la avería")




def noti_recuperacion(central_socket, motivo):
    if not central_socket:
        print("\n[MONITOR] No hay conexión con Central")
        return
    try:
        msg= f"CP_RECUPERACION:{monitor_state['cp_id']}:{motivo}"
        send_msg(central_socket, msg)
        print("\n[MONITOR] Recuperación notificada a Central")
        response = receive_msg(central_socket)
    
    except Exception as e:
        print("\n[MONITOR] Error en la notificación de la recuperación")




def conectar_central(central_ip, central_port,cp_id):
    global monitor_state
    central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"[MONITOR] Conectando a la Central ({central_ip}:{central_port})...")
    try:
        central_socket.connect((central_ip, central_port))
        print("[MONITOR] Conectado a Central")

        # construir mensaje inicial con monitor_state
        estado_str = "OK" if not monitor_state["averiado"] else "KO"
        msg_inicial = f"{cp_id} {monitor_state['ubicacion']} {estado_str} 0.30"
        send_msg(central_socket, msg_inicial)

        print(f"[MONITOR] Información inicial enviada: {msg_inicial}")

        return central_socket

    except Exception as e:
        print(f"[MONITOR] Error al conectar con Central: {e}")
        return None

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
            respuesta=receive_msg(s)
            print("Health")
            if respuesta is None:
                if not monitor_state["averiado"]:
                    print("\n[MONITOR] Avería detectada en Engine: Engine no responde")
                    noti_averia(central_socket, "Engine no responde")
                    monitor_state["averiado"]=True


            elif respuesta=="KO":
                    if not monitor_state["averiado"]:
                        print("\n[MONITOR] Avería detectada en Engine: Engine está KO")
                        noti_averia(central_socket, "Engine está KO")
                        monitor_state["averiado"]=True

            elif respuesta=="OK":
                    if monitor_state["averiado"]:
                        print("\n[MONITOR] Avería arreglada en Engine: Engine está OK")
                        noti_recuperacion(central_socket, "Engine está OK")
                        monitor_state["averiado"]=False
            time.sleep(HEALTHSTATUS_TIEMPO)

        except ConnectionResetError:
            print("\n[MONITOR] Conexion con Engine perdida")

        except Exception as e:
            print(f"\n[MONITOR] Error en healthstatus: {e}")
            time.sleep(HEALTHSTATUS_TIEMPO)


    
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



    engine_socket = EngineConnector(engine_ip, engine_port,cp_id)
    engine_socket.start()
    central_socket = conectar_central(central_ip, central_port, cp_id)
    healthstatus_periodico(engine_socket, central_socket)