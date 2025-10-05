import socket
import threading
import time
import sys

FORMAT = 'utf-8'
HEADER = 64
FIN = "FIN"
HEALTHSTATUS_TIEMPO = 1 #(segundos)

monitor_status = {
    "cp_id": None,
    "ubicacion": None,
    "averiado": False
}

def send_msg(socket, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    socket.send(send_length)
    socket.send(message)

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




def conectar_central(central_ip, central_port):
    central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        central_socket.connect((central_ip, central_port))
        print(f"[MONITOR] Conectando al la Central ({central_ip}:{central_port})...")

        respuesta=central_socket.recv(4096)
        print(respuesta.decode(FORMAT))
        #AQUI TENDRIAMOS QUE MANDAR LA INFORMACIÓN DE NUESTRA CP A LA CENTRAL
        #[ID] [Ubicación] [Estado] [Precio]
        mens =""
        mens= input()
        send_msg(mens)
        #Con el método send, enviamos el mensaje
        #Cerramos la instancia del cliente servidor
        respuesta=central_socket.recv(4096)
        
        print(respuesta.decode(FORMAT))
        central_socket.close()

        print("Conexión cerrada")

    except Exception as e:
        print("Error al conectar:", e)

    central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    central_socket.connect((central_ip, central_port))

    return central_socket


def conectar_engine(engine_ip, engine_port):
    print(f"[MONITOR] Conectando al Engine ({engine_ip}:{engine_port})...")
    engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    engine_socket.connect((engine_ip, engine_port))

    return engine_socket

def healthstatus_periodico(engine_socket, central_socket):
    global monitor_state
    print("\n[MONITOR] Empezando healthcecks periodicos\n")

    while True:
        try:
            send_msg(engine_socket, "HEALTHSTATUS")
            respuesta=receive_msg(engine_socket)

            if respuesta is None:
                if not monitor_status["averiado"]:
                    print("\n[MONITOR] Avería detectada en Engine: Engine no responde")
                    noti_averia(central_socket, "Engine no responde")
                    monitor_status["averiado"]=True


            elif respuesta=="KO":
                    if not monitor_status["averiado"]:
                        print("\n[MONITOR] Avería detectada en Engine: Engine está KO")
                        noti_averia(central_socket, "Engine está KO")
                        monitor_status["averiado"]=True

            elif respuesta=="OK":
                    if monitor_status["averiado"]:
                        print("\n[MONITOR] Avería arreglada en Engine: Engine está OK")
                        noti_recuperacion(central_socket, "Engine está OK")
                        monitor_status["averiado"]=False
            time.sleep(HEALTHSTATUS_TIEMPO)

        except ConnectionResetError:
            print("\n[MONITOR] Conexion con Engine perdida")
            break

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

    central_socket = conectar_central(central_ip, central_port)
    #engine_socket = conectar_engine(engine_ip, engine_port)
    #registro?
    #healthstatus_periodico(engine_socket, central_socket)