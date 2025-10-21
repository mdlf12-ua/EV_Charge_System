import socket
import threading
import time
import sys

FORMAT = 'utf-8'
HEADER = 64
MAX_CONEXIONES = 20
FIN = "FIN"

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

def receive_msg(socket):
    try:
        length = int(socket.recv(HEADER).decode(FORMAT).strip())
        return socket.recv(length).decode(FORMAT)
    except:
        return None

def engine_bucle(conn, ip):

    global cp_state

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)

        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)

            if msg == FIN:
                connected = False
                print("[ENGINE] Monitor desconectado")

            elif msg=="HEALTHSTATUS":
                send_msg(conn, cp_state["health_status"])

            elif msg=="STOP":

                print("[ENGINE] Central ordena que paremos")
                # if cp_state["suministro_activo"]:
                #     cp_state["suministro_activo"]=
                cp_state["status"]="PARADO"
                cp_state["health_status"] = "OK"
                print("[ENGINE] CP Parado por central")
                #kafka
                send_msg(conn, "OK")



            elif msg=="CONTINUE":

                print("[ENGINE] Central ordena que reanudemos")
                cp_state["status"]="ACTIVADO"
                cp_state["health_status"] = "OK"
                print("[ENGINE] CP Parado por central")
                #kafka
                send_msg(conn, "OK")

            #msg==registrar

            else:
                print("[ENGINE] Mensaje no reconocido")
                send_msg(conn, "ERROR: Mensaje no reconocido")

    print("CERRANDO CONEXIÓN CON EL CLIENTE")
    conn.close()



def start_socket_monitor(ip, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((ip, port))
    server.listen()
    print(f"[ENGINE] Servidor a la escucha en {ip}")

    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    while True:

        try:

            print(f"\n[ENGINE] Conectando al Monitor {ip}:{port}...")
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((ip, port))
            print("[ENGINE] Conectado al Monitor, a la espera de mensajes\n")

            connected = True
            while connected:

                msg = receive_msg(client)

                if not msg:
                    print("[ENGINE] Monitor desconectado, no hay conexión")
                    return

                if msg == FIN:
                    connected = False
                    print("[ENGINE] Monitor desconectado con FIN")

                elif msg=="HEALTHSTATUS":
                    send_msg(conn, cp_state["health_status"])

                elif msg=="STOP":

                    print("[ENGINE] Central ordena que paremos")
                    # if cp_state["suministro_activo"]:
                    #     cp_state["suministro_activo"]=
                    cp_state["status"]="PARADO"
                    cp_state["health_status"] = "OK"
                    print("[ENGINE] CP Parado por central")
                    #kafka
                    send_msg(conn, "OK")



                elif msg=="CONTINUE":

                    print("[ENGINE] Central ordena que reanudemos")
                    cp_state["status"]="ACTIVADO"
                    cp_state["health_status"] = "OK"
                    print("[ENGINE] CP Parado por central")
                    #kafka
                    send_msg(conn, "OK")

                    #msg==registrar

                else:
                    print("[ENGINE] Mensaje no reconocido")
                    send_msg(conn, "ERROR: Mensaje no reconocido")



        except ConnectionRefusedError:
            print(f"[ENGINE] No se puede conectar con el Monitor ({ip}:{port})")
            return
        except Exception as e:
            print(f"[ENGINE] Error: {e}")
            return


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("ERROR Argumentos: EV_CP_E.py <IP> <PORT>")
        sys.exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])
    start_socket_monitor(ip, port)