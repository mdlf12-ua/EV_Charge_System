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
        topic=message.topic

        if topic=='cp-ordenes':
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
        
        elif topic=='autorizacion-suministro':
            print(f"\n[ENGINE] Autorizado suministro en {cp_id}")
            cp_state["status"] = "AUTORIZADO"
            cp_state["conductor_id"] = data.get("conductor_id")
            print(f"[ENGINE] Esperando que el conductor enchufe su vehículo\n")
            menu_local_cp()

        else:
            print(f"[ENGINE] Mensaje recibido pero topic {topic} no reconocido")

    except Exception as e:
        print(f"[ENGINE] Error procesando mensaje Kafka: {e}")    


def menu_local_cp():

    print("====================================")
    print("MENÚ LOCAL DEL CHARGING POINT")
    print("====================================")
    print(f"CP ID: {cp_state['cp_id']}")
    print(f"Estado: {cp_state['status']}")
    print("====================================")
    
    while True:
        print("\nOpciones:")
        print("  1. Enchufar vehículo (iniciar suministro)")
        print("  2. Desenchufar vehículo (finalizar suministro)")
        print("  0. Salir")
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

            elif opcion == "0":
                print("[ENGINE] Cerrando menú...")
                break

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
        print(f"[ENGINE] ⚠️  CP no está autorizado (estado: {cp_state['status']})")
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

        cp_state["consumo_kw"] += 0.5
        cp_state["importe_euro"] = cp_state["consumo_kw"] * cp_state["precio_kwh"]
        

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

    print(f"\n\n[ENGINE] 🏁 FINALIZANDO SUMINISTRO")
    print(f"[ENGINE] Consumo total: {cp_state['consumo_kw']:.2f} kWh")
    print(f"[ENGINE] Importe total: {cp_state['importe_euro']:.2f} €")
    print(f"[ENGINE] Duración: {duracion:.0f} segundos\n")
    

    send_to_kafka('cp-estado', {
        "type": "suministro_finalizado",
        "cp_id": cp_state["cp_id"],
        "conductor_id": cp_state["conductor_id"],
        "consumo_total": round(cp_state["consumo_kw"], 2),
        "importe_total": round(cp_state["importe_euro"], 2),
        "duracion": round(duracion, 0),
        "timestamp": time.time()
    })
    

    cp_state["suministro_activo"] = False
    cp_state["conductor_id"] = None
    cp_state["status"] = "ACTIVADO"
    cp_state["consumo_kw"] = 0.0
    cp_state["importe_euro"] = 0.0

def kafka_consumer_thread(kafka_broker, cp_id):

    global kafka_consumer
    print(f"[ENGINE] Iniciando consumidor Kafka")

    try:
        kafka_consumer= KafkaConsumer(
            'cp-ordenes',
            'autorizacion-suministro', #Esto es el asunto del mensaje
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
    

    