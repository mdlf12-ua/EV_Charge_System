import socket
import threading
import time
import sys
from kafka import KafkaConsumer 
import json
from kafka import KafkaProducer
#Tipo mensajes kafka: cp-estado
#                     cp-ordenes
#

FORMAT = 'utf-8'
HEADER = 64
MAX_CONEXIONES = 20
FIN = "FIN"
TIMEOUT=5000 #in miliseconds

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
                cp_state["status"]="PARADO"
                cp_state["health_status"] = "OK"
                print("[ENGINE] CP Parado por socket")
                send_msg(conn, "OK")



            elif msg=="CONTINUE":

                print("[ENGINE] Central ordena que reanudemos")
                cp_state["status"]="ACTIVADO"
                cp_state["health_status"] = "OK"
                print("[ENGINE] CP Parado por socket")
                send_msg(conn, "OK")

            #msg==registrar

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


def iniciar_kafka_producer(broker):
    global kafka_producer

    try:

        kafka_producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8') #Formato JSON
        )

        print(f"[ENGINE] Productor Kafka conectado a {broker}")

    except Exception as e:

        print(f"[ENGINE] Error conectando productor Kafka: {e}")

def send_to_kafka(asunto, message):

    global kafka_producer

    try:
        if kafka_producer:

            kafka_producer.send(asunto, value=message)

            kafka_producer.flush() #El flush manda lo que tiene por si acaso
            print(f"[ENGINE] Mensaje enviado a {asunto}: {message}")
        else:
            print(f"[ENGINE] Productor Kafka no inicializado")

    except Exception as e:
        print(f"[ENGINE] Error mandando mensaje Kafka: {e}")

def handle_kafka_message(message):
    global cp_state

    try:

        data=message.value
        message_type=data.get("type")
        cp_id=data.get("cp_id")

        if cp_id != cp_state["cp_id"]:
            return
        
        if message_type == "STOP":

            print(f"\n[ENGINE] STOP recibido desde CENTRAL mediante Kafka")

            if cp_state["suministro_activo"]:
                print("[ENGINE] Deteniendo suministro activo...")
                cp_state["suministro_activo"] = False
                stop_suministro.set()

                cp_state["status"] = "PARADO"
                print(f"[ENGINE] Estado cambiado a: {cp_state['status']}")
                print("[ENGINE] CP fuera de servicio (OoO)\n")

                #Notificar a Central
                send_to_kafka('cp-estado', {
                "cp_id": cp_state["cp_id"],
                "status": "PARADO",
                "timestamp": time.time(),
                "reason": "orden_central"
                })

        elif message_type=="CONTINUE":

            print(f"\n[ENGINE] REANUDAR recibido desde CENTRAL mediante Kafka")
            cp_state["status"] = "ACTIVADO"
            print(f"[ENGINE] Estado cambiado a: {cp_state['status']}")
            print("[ENGINE] CP activado y disponible\n")

            #Notificar a Central
            send_to_kafka('cp-estado', {
                "cp_id": cp_state["cp_id"],
                "status": "ACTIVADO",
                "timestamp": time.time(),
                "reason": "orden_central"
                })


        else:
            print("[ENGINE] Mensaje recibido pero no reconocido")

    except Exception as e:
        print(f"[ENGINE] Error procesando mensaje Kafka: {e}")    




def kafka_consumer_thread(kafka_broker, cp_id):

    global kafka_consumer
    print(f"[ENGINE] Iniciando consumidor Kafka")

    try:
        kafka_consumer= KafkaConsumer(
            'cp-ordenes', #Esto es el asunto del mensaje
            bootstrap_servers=kafka_broker,
            group_id=f'engine-{cp_id}', #Pone a los cp en un grupo de consumidores
            value_deserializer=lambda m: json.loads(m.decode('utf-8')), #El mensaje se recibe en formato JSON
            auto_offset_reset='latest', #Si se resetea solo empieza a leer los mensajes nuevos
            enable_auto_commit=True, #Por si el cp se reinicia para saber por donde continuar
            consumer_timeout_ms=TIMEOUT
            )

        print(f"[ENGINE] Consumidor Kafka conectado\n")

        while True:

            kafka_data=kafka_consumer.poll(TIMEOUT)

            if not kafka_data:
                continue
            else:
                for tp, messages in kafka_data.items():
                    for message in messages:
                        handle_kafka_message(message)

    except Exception as e:
        print(f"[ENGINE] Error en Consumidor Kafka: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Uso: python EV_CP_E.py <IP> <PORT> <KAFKA_BROKER> <CP_ID>")
        sys.exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])
    kafka_broker = sys.argv[3]
    cp_id = sys.argv[4]


    cp_state["cp_id"] = cp_id
    cp_state["status"] = "ACTIVADO"

    print(f"[ENGINE] - Punto de Recarga {cp_id}\n")
    print("----------------------------------------\n")
    print(f"Socket Monitor: {ip}:{port}\n")
    print(f"Kafka Broker: {kafka_broker}\n")

    iniciar_kafka_producer(kafka_broker)

    kafka_thread = threading.Thread(
        target=kafka_consumer_thread, 
        args=(kafka_broker, cp_id),
        daemon=True #El daemon hace que cuando acabe el hilo se borre
    )
    kafka_thread.start()

    print(f"[ENGINE] Registrando CP en la central...\n")
    send_to_kafka('cp-register', {
        "cp_id": cp_id,
        "status": "ACTIVADO",
        "timestamp": time.time(),
        "ubicacion": "X",
        "precio_kwh": 0.30
    })

    start_socket_monitor(ip, port)

    