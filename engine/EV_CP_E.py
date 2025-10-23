import socket
import threading
import time
import sys

FORMAT = 'utf-8'
HEADER = 64
MAX_CONEXIONES = 20
FIN = "FIN"
SERVER = "0.0.0.0"
PORT = 6000
ADDR = (SERVER, PORT)

kafka_producer = None
kafka_consumer = None
kafka_broker = None
suministro_thread = None
stop_suministro = threading.Event()

cp_state = {
    "status": "DESCONECTADO",  
    "cp_id": None,
    "ubicacion": None,
    "precio_kwh": None,  
    "health_status": "OK",  
    "suministro_activo": False,
    "conductor_id": None,
    "consumo_kw": 0.0,
    "importe_euro": 0.0,
}

def send_msg(conn, msg):
    """Envía un mensaje con header de longitud"""
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    conn.send(send_length)
    conn.send(message)

def receive_msg(conn):
    """Recibe un mensaje con encabezado de longitud fija (HEADER)."""
    try:
        # Recibimos la longitud del mensaje
        msg_length = conn.recv(HEADER).decode(FORMAT).strip()
        if not msg_length:
            return None

        msg_length = int(msg_length)
        # Ahora recibimos el mensaje real
        msg = conn.recv(msg_length).decode(FORMAT)
        return msg
    except Exception as e:
        print(f"[ENGINE] Error al recibir mensaje: {e}")
        return None


def handle_client(conn, ip):
    global cp_state
    print(f"[NUEVA CONEXION] {ip} connected.")

    registered = False
    connected = True

    while connected:
        msg = receive_msg(conn)
        if msg is None:  # desconexión limpia o error de lectura
            break

        # --- 1) Registro inicial: espera "CP_ID:<id>" ---
        if not registered:
            if msg.startswith("CP_ID:"):
                cp_id = msg.split(":", 1)[1].strip()
                cp_state["cp_id"] = cp_id
                print(f"[ENGINE] CP registrado: {cp_id}")
                send_msg(conn, "OK")          # ACK al monitor
                registered = True

                # (opcional) devolver snapshot inicial de estado
                # ej: send_msg(conn, f"STATE:{cp_state['status']}:{cp_state['health_status']}")
                continue
            else:
                # si llega otra cosa antes del CP_ID, la rechazamos
                print(f"[ENGINE] Esperaba CP_ID:<id>, recibido: {msg!r}")
                send_msg(conn, "ERROR: Primero envía CP_ID:<id>")
                # puedes decidir: o seguir esperando, o cortar:
                # connected = False
                continue

        # --- 2) Protocolo normal tras estar registrado ---
        if msg == FIN:
            connected = False
            print("[ENGINE] Monitor desconectado")

        elif msg == "HEALTHSTATUS":
            send_msg(conn, cp_state["health_status"])

        elif msg == "STOP":
            print("[ENGINE] Central ordena que paremos")
            cp_state["status"] = "PARADO"
            cp_state["health_status"] = "OK"
            print("[ENGINE] CP Parado por central")
            send_msg(conn, "OK")

        elif msg == "CONTINUE":
            print("[ENGINE] Central ordena que reanudemos")
            cp_state["status"] = "ACTIVADO"
            cp_state["health_status"] = "OK"
            print("[ENGINE] CP reanudado por central")
            send_msg(conn, "OK")

        else:
            print(f"[ENGINE] Mensaje no reconocido: {msg!r}")
            send_msg(conn, "ERROR: Mensaje no reconocido")

    print("CERRANDO CONEXIÓN CON EL CLIENTE")
    conn.close()



def start_socket_monitor():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[ENGINE] Servidor a la escucha en {SERVER}")

    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count()
        if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1





if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python EV_CP_E.py <IP> <PORT>")
        sys.exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])
    start_socket_monitor()