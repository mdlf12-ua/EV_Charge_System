import socket
import threading
import time
import sys
import json
from kafka import KafkaProducer, KafkaConsumer
import os
import logging
import ssl

TLS_ENABLED = os.getenv("TLS_ENABLED", "1") == "1"
TLS_CERT = os.getenv("TLS_CERT", "/app/certs/certServ.pem")

_tls_ctx = None
if TLS_ENABLED:
    _tls_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    # certServ.pem contiene cert + private key (como el ejemplo del profe)
    _tls_ctx.load_cert_chain(certfile=TLS_CERT, keyfile=TLS_CERT)

# === CONFIGURACIÓN DE LOGS ===
os.makedirs("logs", exist_ok=True)

engine_name = os.getenv("ENGINE_NAME", "engine_default")

logger = logging.getLogger(engine_name)
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')

# Cada engine escribe en su propio archivo dentro de logs/
log_path = f"logs/{engine_name}.log"
file_handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Silenciar librerías ruidosas
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


PORT = 6000
SERVER = "0.0.0.0"
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
HEADER = 64
MAX_CONEXIONES = 20
TIMEOUT = 5000 #In miliseconds
UBICACION="Yakutsk"
PRECIO=1
CONSUMO=0.0
KWH=3600.0


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
        logger.info(f"[ENGINE] Error al recibir mensaje: {e}")
        return None


def handle_client(conn, addr):
    global cp_state

    logger.info(f"[ENGINE] Nueva conexión desde {addr}")

    # Esperamos mensaje de registro: "CP_ID:cp_id" o "CP_ID:cp_id:encryption_key"
    msg = receive_msg(conn)
    if not msg or not msg.startswith("CP_ID:"):
        send_msg(conn, "ERROR: Debes enviar CP_ID:<id> o CP_ID:<id>:<encryption_key>")
        conn.close()
        return

    partes = msg.split(":", 2)
    cp_id = partes[1].strip()
    encryption_key = partes[2].strip() if len(partes) > 2 else None
    
    cp_state["cp_id"] = cp_id
    cp_state["consumo_kw"] = 0.0
    cp_state["precio_kwh"] = PRECIO
    cp_state["importe_euro"] = 0.0
    
    logger.info(f"[ENGINE] CP_ID = {cp_id}")
    
    # Verificar si tiene clave de cifrado (está autenticado)
    if encryption_key:
        logger.info(f"[ENGINE] CP autenticado con clave de cifrado")
        cp_state["status"] = "ACTIVADO"
        cp_state["authenticated"] = True
    else:
        logger.warning(f"[ENGINE] CP NO autenticado (sin clave de cifrado)")
        cp_state["status"] = "REGISTRADO"  # Estado intermedio
        cp_state["authenticated"] = False
    
    send_msg(conn, "OK")

    logger.info(f"[ENGINE] - Punto de Recarga {cp_id}")
    logger.info("----------------------------------------")
    logger.info(f"Socket Monitor: {SERVER}:{PORT}")
    logger.info(f"Kafka Broker: {kafka_broker}")
    logger.info(f"Estado: {cp_state['status']}")
    logger.info(f"Autenticado: {cp_state['authenticated']}\n")

    # Iniciar Kafka
    iniciar_kafka_producer(kafka_broker)
    kafka_thread = threading.Thread(
        target=kafka_consumer_thread, 
        args=(kafka_broker, cp_id),
        daemon=True
    )
    kafka_thread.start()

    # SOLO registrar en Central si está autenticado
    if cp_state["authenticated"]:
        logger.info(f"[ENGINE] Registrando CP autenticado en Central...\n")
        send_to_kafka('cp-register', {
            "cp_id": cp_id,
            "status": "ACTIVADO",
            "timestamp": time.time(),
            "ubicacion": UBICACION,
            "consumo_kw": cp_state["consumo_kw"],
            "importe_euro": cp_state["importe_euro"],
            "precio_kwh": cp_state["precio_kwh"]
        })
    else:
        logger.warning(f"[ENGINE] CP NO autenticado - NO se registra en Central")
        logger.warning(f"[ENGINE] Debe completar: Registro → Autenticación → Reconexión\n")

    # Bucle principal
    while True:
        msg = receive_msg(conn)
        if not msg:
            break

        if msg == "HEALTHSTATUS":
            send_msg(conn, cp_state["health_status"])
        
        elif msg.startswith("AUTHENTICATED:"):
            # Monitor notifica que se autenticó y envía la clave
            _, new_encryption_key = msg.split(":", 1)
            logger.info(f"[ENGINE] Recibida clave de autenticación del Monitor")
            cp_state["authenticated"] = True
            cp_state["status"] = "ACTIVADO"
            
            # AHORA SÍ, registrar en Central
            logger.info(f"[ENGINE] Registrando CP autenticado en Central...\n")
            send_to_kafka('cp-register', {
                "cp_id": cp_id,
                "status": "ACTIVADO",
                "timestamp": time.time(),
                "ubicacion": UBICACION,
                "consumo_kw": cp_state["consumo_kw"],
                "importe_euro": cp_state["importe_euro"],
                "precio_kwh": cp_state["precio_kwh"]
            })
        
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

    logger.warning("[ENGINE] Monitor desconectado")
    conn.close()




def start_socket_monitor():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    logger.info(f"[ENGINE] Escuchando en {SERVER}:{PORT} (TLS={'ON' if TLS_ENABLED else 'OFF'})")

    while True:
        conn, addr = server.accept()

        if TLS_ENABLED:
            try:
                conn = _tls_ctx.wrap_socket(conn, server_side=True)
            except ssl.SSLError as e:
                logger.warning(f"[ENGINE] Handshake TLS fallido desde {addr}: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
                continue

        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


def iniciar_kafka_producer(broker):
    global kafka_producer

    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"[ENGINE] Productor Kafka conectado a {kafka_broker}")
    except Exception as e:
        logger.error(f"[ENGINE] Error conectando productor Kafka: {e}")
        return

def send_to_kafka(asunto, message):

    global kafka_producer

    try:
        if kafka_producer:

            kafka_producer.send(asunto, value=message)

            kafka_producer.flush() #El flush manda lo que tiene por si acaso
            logger.info(f"[ENGINE] Mensaje enviado a {asunto}: {message}")
        else:
            logger.warning(f"[ENGINE] Productor Kafka no inicializado")

    except Exception as e:
        logger.error(f"[ENGINE] Error mandando mensaje Kafka: {e}")

def handle_kafka_message(message):
    global cp_state

    try:

        data=message.value
        message_type=data.get("type")
        cp_id=data.get("cp_id")
        topic=message.topic

        logger.info(f"[ENGINE DEBUG] Topic: {topic}, Type: {message_type}, CP_ID msg: {cp_id}, CP_ID local: {cp_state['cp_id']}")

        if topic==f'cp-ordenes-{cp_id}':
            if cp_id != cp_state["cp_id"]:
                logger.warning(f"[ENGINE] Mensaje no es para este CP (esperado: {cp_state['cp_id']}, recibido: {cp_id})")
                return
            
            if message_type == "STOP":

                logger.info(f"\n[ENGINE] STOP recibido desde CENTRAL mediante Kafka")

                if cp_state["suministro_activo"]:
                    logger.info("[ENGINE] Deteniendo suministro activo...")
                    stop_suministro.set()
                    cp_state["suministro_activo"] = False
                    time.sleep(1)

                estado_anterior = cp_state["status"]
                cp_state["status"] = "PARADO"
                logger.info(f"[ENGINE] Estado cambiado: {estado_anterior} -> {cp_state['status']}")
                logger.warning("[ENGINE] CP fuera de servicio (OoO)\n")

                    #Notificar a Central
                send_to_kafka('cp-estado', {
                    "cp_id": cp_state["cp_id"],
                    "status": "PARADO",
                    "timestamp": time.time(),
                    "reason": "orden_central"
                })

 


            elif message_type=="CONTINUE":

                logger.info(f"\n[ENGINE] REANUDAR recibido desde CENTRAL mediante Kafka")
                cp_state["status"] = "ACTIVADO"
                logger.info(f"[ENGINE] Estado cambiado a: {cp_state['status']}")
                logger.info("[ENGINE] CP activado y disponible\n")

                estado_anterior = cp_state["status"]
                cp_state["status"] = "ACTIVADO"
                logger.info(f"[ENGINE] Estado cambiado: {estado_anterior} -> {cp_state['status']}")
                logger.info("[ENGINE] CP ACTIVADO Y DISPONIBLE\n")

                #Notificar a Central
                send_to_kafka('cp-estado', {
                    "cp_id": cp_state["cp_id"],
                    "status": "ACTIVADO",
                    "timestamp": time.time(),
                    "reason": "orden_central"
                })


            else:
                logger.warning("[ENGINE] Mensaje recibido pero no reconocido")
        
        elif topic==f'autorizacion-suministro-{cp_id}':
            logger.info(f"\n[ENGINE] Autorizado suministro en {cp_id}")
            cp_state["status"] = "AUTORIZADO"
            cp_state["conductor_id"] = data.get("conductor_id")
            print(f"[ENGINE] Esperando que el conductor enchufe su vehículo\n")

        else:
            logger.warning(f"[ENGINE] Mensaje recibido pero topic {topic} no reconocido")

    except Exception as e:
        logger.error(f"[ENGINE] Error procesando mensaje Kafka: {e}")    


def menu_local_cp():

    print("====================================")
    print("MENÚ LOCAL DEL CHARGING POINT")
    print("====================================")
    print(f"CP ID: {cp_state['cp_id']}")
    print(f"Estado: {cp_state['status']}")
    print("====================================")
    
    while True:

        print(f"CP ID: {cp_state['cp_id']}")
        print(f"Estado: {cp_state['status']}")
        print("\nOpciones:")
        print("  1. Enchufar vehículo (iniciar suministro)")
        print("  2. Desenchufar vehículo (finalizar suministro)")
        print("  3. Simular averia")
        print("  0. Salir")
        print("  r. Refrescar")
        print("-------------------------------------------------------")
        
        try:
            opcion = input("\nSelecciona una opción: ").strip()
            
            if opcion == "1":
                if cp_state["status"] == "AUTORIZADO":
                    if cp_state["conductor_id"]:
                        iniciar_suministro(cp_state["conductor_id"])
                    else:
                        print("[ENGINE] No hay conductor autorizado")
                else:
                    print(f"[ENGINE] CP no está autorizado (estado: {cp_state['status']})")
                    print("[ENGINE] Primero debe recibir autorización de Central")
                    
            elif opcion == "2":
                
                if cp_state["suministro_activo"]:
                    print("[ENGINE] Deteniendo suministro...")
                    stop_suministro.set()
                    cp_state["suministro_activo"] = False
                else:
                    print("[ENGINE] No hay suministro activo")
            elif opcion == "3":
                print("Simulando Averia")
                cp_state["health_status"] = "KO"
                cp_state["status"] = "Averiado"
            elif opcion == "0":

                if cp_state["suministro_activo"]:
                    print("[ENGINE] Suministro activo, desenchufe primero el vehiculo para salir de la aplicación")
                else:
                    print("[ENGINE] Cerrando menú...")
                    break
            elif opcion == "r":
                continue

            else:
                print("[ENGINE] Opcion no reconocida")

        except KeyboardInterrupt:
            print("\n[ENGINE] Interrumpido")
            break
        except Exception as e:
            print(f"[ENGINE] Error: {e}")

def iniciar_suministro(conductor_id):
    global cp_state, suministro_thread, stop_suministro

    if cp_state["suministro_activo"]:
        print("[ENGINE] Ya hay un suministro activo")
        return False
    if cp_state["status"] != "AUTORIZADO":
        print(f"[ENGINE] CP no está autorizado (estado: {cp_state['status']})")
        return False
    print(f"\n[ENGINE] INICIANDO SUMINISTRO para conductor {conductor_id}")

    cp_state["suministro_activo"] = True
    cp_state["conductor_id"] = conductor_id
    cp_state["status"] = "SUMINISTRANDO"
    cp_state["consumo_kw"] = 0.0
    cp_state["importe_euro"] = 0.0

    stop_suministro.clear()

    send_to_kafka('cp-estado', {
        "type": "suministro_iniciado",
        "cp_id": cp_state["cp_id"],
        "conductor_id": conductor_id,
        "status": "SUMINISTRANDO",
        "timestamp": time.time()
    })

    suministro_thread = threading.Thread(target=thread_suministro, daemon=True)
    suministro_thread.start()


def thread_suministro():
    global cp_state

    tiempo_inicio=time.time()

    while cp_state["suministro_activo"] and not stop_suministro.is_set():

        cp_state["consumo_kw"] += KWH/3600
        cp_state["importe_euro"] = cp_state["consumo_kw"] + cp_state["precio_kwh"]
        

        send_to_kafka('cp-telemetria', {
            "type": "telemetria",
            "cp_id": cp_state["cp_id"],
            "conductor_id": cp_state["conductor_id"],
            "consumo_kw": round(cp_state["consumo_kw"], 2),
            "importe_euro": round(cp_state["importe_euro"], 2),
            "timestamp": time.time()
        })
        
        print(f"\r[ENGINE] Suministrando: {cp_state['consumo_kw']:.2f} kW | {cp_state['importe_euro']:.2f} €", end='', flush=True)
        
        time.sleep(1)

    duracion = time.time() - tiempo_inicio
    finalizar_suministro(duracion)

    
def finalizar_suministro(duracion):
    global cp_state

    print(f"\n\n[ENGINE] FINALIZANDO SUMINISTRO")
    print(f"[ENGINE] Consumo total: {cp_state['consumo_kw']:.2f} kWh")
    print(f"[ENGINE] Importe total: {cp_state['importe_euro']:.2f} €")
    print(f"[ENGINE] Duración: {duracion:.0f} segundos\n")
    
    cp_state["status"] = "ACTIVADO"



    send_to_kafka('cp-estado', {
        "type": "suministro_finalizado",
        "status":  cp_state["status"],
        "cp_id": cp_state["cp_id"],
        "conductor_id": cp_state["conductor_id"],
        "consumo_total": round(cp_state["consumo_kw"], 2),
        "importe_total": round(cp_state["importe_euro"], 2),
        "duracion": round(duracion, 0),
        "timestamp": time.time()
    })
    
    cp_state["suministro_activo"] = False
    cp_state["conductor_id"] = None
    cp_state["consumo_kw"] = 0.0
    cp_state["importe_euro"] = 0.0

def kafka_consumer_thread(kafka_broker, cp_id):

    global kafka_consumer
    logger.info(f"[ENGINE] Iniciando consumidor Kafka")

    try:
        kafka_consumer= KafkaConsumer(
            f'cp-ordenes-{cp_id}',
            f'autorizacion-suministro-{cp_id}', #Esto es el asunto del mensaje
            bootstrap_servers=kafka_broker,
            group_id=f'engine-{cp_id}', #Pone a los cp en un grupo de consumidores
            value_deserializer=lambda m: json.loads(m.decode('utf-8')), #El mensaje se recibe en formato JSON
            auto_offset_reset='latest', #Si se resetea solo empieza a leer los mensajes nuevos
            enable_auto_commit=True, #Por si el cp se reinicia para saber por donde continuar
            consumer_timeout_ms=TIMEOUT
            )

        logger.info(f"[ENGINE] Consumidor Kafka conectado\n")

        while True:

            kafka_data=kafka_consumer.poll(TIMEOUT)

            if not kafka_data:
                continue
            else:
                for tp, messages in kafka_data.items():
                    for message in messages:
                        handle_kafka_message(message)

    except Exception as e:
        logger.error(f"[ENGINE] Error en Consumidor Kafka: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Uso: python EV_CP_E.py <KAFKA_BROKER>")
        sys.exit(1)
    


    kafka_broker = sys.argv[1]
    
    logger.info(f"[ENGINE] Esperando conexión de Monitor...\n")
    logger.info(f"Socket Monitor: {SERVER}:{PORT}")
    logger.info(f"Kafka Broker: {kafka_broker}\n")

    socket_thread = threading.Thread(target=start_socket_monitor, daemon=True)
    socket_thread.start()

    time.sleep(3)

    try:
        menu_local_cp()

    except KeyboardInterrupt:
        logger.warning("\n[ENGINE] Apagando...")
    

    
