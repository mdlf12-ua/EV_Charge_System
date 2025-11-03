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

PORT = 6000
SERVER = "0.0.0.0"
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
HEADER = 64
MAX_CONEXIONES = 20
TIMEOUT = 5000 #In miliseconds

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
    message = msg.encode(FORMAT)
    msg_length = len(message)
    header = f"{msg_length:<{HEADER}}".encode(FORMAT)
    conn.send(header + message)

def receive_msg(conn):
    try:
        msg_length = conn.recv(HEADER).decode(FORMAT).strip()
        if not msg_length:
            return None
        msg = conn.recv(int(msg_length)).decode(FORMAT)
        return msg
    except Exception as e:
        print(f"[ENGINE] Error al recibir mensaje: {e}")
        return None


def handle_client(conn, addr):
    global cp_state

    print(f"[ENGINE] Nueva conexión desde {addr}")

    # Esperamos mensaje de registro
    msg = receive_msg(conn)
    if not msg or not msg.startswith("CP_ID:"):
        send_msg(conn, "ERROR: Debes enviar CP_ID:<id>")
        conn.close()
        return

    cp_id = msg.split(":", 1)[1].strip()
    cp_state["cp_id"] = cp_id
    cp_state["status"] = "ACTIVADO"
    print(f"[ENGINE] Registrado CP_ID = {cp_id}")

    send_msg(conn, "OK")



    cp_state["cp_id"] = cp_id
    cp_state["status"] = "ACTIVADO"

    print(f"[ENGINE] - Punto de Recarga {cp_id}\n")
    print("----------------------------------------\n")
    print(f"Socket Monitor: {SERVER}:{PORT}\n")
    print(f"Kafka Broker: {kafka_broker}\n")



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

    # Ahora podemos iniciar Kafka
    #iniciar_kafka(cp_id, kafka_broker)

    # Bucle principal
    while True:
        msg = receive_msg(conn)
        if not msg:
            break

        if msg == "HEALTHSTATUS":
            send_msg(conn, cp_state["health_status"])
        elif msg == "STOP":
            cp_state["status"] = "PARADO"
            send_msg(conn, "OK")
        elif msg == "CONTINUE":
            cp_state["status"] = "ACTIVADO"
            send_msg(conn, "OK")
        elif msg == "FIN":
            break
        else:
            send_msg(conn, "ERROR: comando desconocido")

    print("[ENGINE] Monitor desconectado")
    conn.close()




def start_socket_monitor():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[ENGINE] Escuchando en {SERVER}:{PORT}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


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
    if len(sys.argv) != 2:
        print("Uso: python EV_CP_E.py <KAFKA_BROKER>")
        sys.exit(1)
    


    kafka_broker = sys.argv[1]
    
    print(f"[ENGINE] Esperando conexión de Monitor...\n")
    print(f"Socket Monitor: {SERVER}:{PORT}")
    print(f"Kafka Broker: {kafka_broker}\n")

    start_socket_monitor()



    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[ENGINE] Apagando...")
    

    