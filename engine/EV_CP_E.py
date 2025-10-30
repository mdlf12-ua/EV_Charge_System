import socket
import threading
import time
import sys
import json
from kafka import KafkaProducer, KafkaConsumer

PORT = 6000
SERVER = "0.0.0.0"
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
HEADER = 64
MAX_CONEXIONES = 20
TIMEOUT = 5000

kafka_producer = None
kafka_consumer = None
kafka_broker = None
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


def iniciar_kafka(cp_id, kafka_broker):
    """Inicia productor, consumidor y registra CP."""
    global kafka_producer

    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"[ENGINE] Productor Kafka conectado a {kafka_broker}")
    except Exception as e:
        print(f"[ENGINE] Error conectando productor Kafka: {e}")
        return

    # Lanza el hilo consumidor
    kafka_thread = threading.Thread(
        target=kafka_consumer_thread,
        args=(kafka_broker, cp_id),
        daemon=True
    )
    kafka_thread.start()

    # Notifica registro
    send_to_kafka('cp-register', {
        "cp_id": cp_id,
        "status": "ACTIVADO",
        "timestamp": time.time(),
        "ubicacion": "X",
        "precio_kwh": 0.30
    })


def send_to_kafka(topic, message):
    if kafka_producer:
        try:
            kafka_producer.send(topic, value=message)
            kafka_producer.flush()
            print(f"[ENGINE] Mensaje enviado a {topic}: {message}")
        except Exception as e:
            print(f"[ENGINE] Error mandando mensaje Kafka: {e}")
    else:
        print("[ENGINE] Productor Kafka no inicializado")


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

    # Ahora podemos iniciar Kafka
    iniciar_kafka(cp_id, kafka_broker)

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


def kafka_consumer_thread(kafka_broker, cp_id):
    print(f"[ENGINE] Iniciando consumidor Kafka para {cp_id}")
    try:
        consumer = KafkaConsumer(
            'cp-ordenes',
            bootstrap_servers=[kafka_broker],
            group_id=f'engine-{cp_id}',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )

        for message in consumer:
            data = message.value
            if data.get("cp_id") != cp_id:
                continue

            tipo = data.get("type")
            if tipo == "STOP":
                print(f"[ENGINE] STOP recibido por Kafka para {cp_id}")
            elif tipo == "CONTINUE":
                print(f"[ENGINE] CONTINUE recibido por Kafka para {cp_id}")

    except Exception as e:
        print(f"[ENGINE] Error en consumidor Kafka: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python EV_CP_E.py <KAFKA_BROKER>")
        sys.exit(1)

    kafka_broker = sys.argv[1]
    start_socket_monitor()
