import socket 
import threading
from threading import Lock
import mysql.connector
import os
import time
import sys
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
# Topics Kafka:
# CONSUMIDOR: solicitud-recarga (del Driver)
# PRODUCTOR: notificaciones-{driver_id} (al Driver)
# PRODUCTOR: datos-consumo (telemetría al Driver)
# PRODUCTOR: cp-ordenes (al Engine)               

HEADER = 64
PORT = 5000
SERVER = "0.0.0.0"
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 10
RETRIES=3 #Reintentos para Kafka
TIMEOUT=5000 #milisegundos


central_cps = {}
lock = Lock()
kafka_producer = None
kafka_consumer=None

def search_CP():

    DB_HOST = os.getenv("DB_HOST", "mysql")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER", "usuario")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "contraseña")
    DB_NAME = os.getenv("DB_NAME", "database")

    while True:
        try:
            conexion = mysql.connector.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            print("Conectado a MySQL")
            break
        except mysql.connector.Error as err:
            print(f"No se pudo conectar a MySQL, reintentando en 5 segundos... ({err})")
            time.sleep(5)

    cursor = conexion.cursor(dictionary=True)
    cursor.execute("SELECT * FROM ChargingPoint")
    rows = cursor.fetchall()

    for row in rows:
        central_cps[row["ID"]] = {
            "ID": row["ID"],
            "Ubicacion": row["Ubicacion"],
            "PRECIO": row["PRECIO"],
            "ESTADO": 'Parado',
            "CONDUCTOR_ID": row["CONDUCTOR_ID"],
            "CONSUMO_KW": row["CONSUMO_KW"],
            "IMPORTE_EU": row["IMPORTE_EU"]
        }


    conexion.close()
    print(f"[CENTRAL] Cargados {len(central_cps)} CPs desde la BD.")


#Función que utilizara cada hilo para antender a un cliente
def handle_CP(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")
    #############################################
    #Aqui explicariamos al cliente el protocolo #
    #############################################

    #El cliente envia dos mensajes, la longitud real del mensaje:
    while True:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        #si hay mensaje
        if msg_length:
            msg_length = int(msg_length)
            #El mensaje real:
            msg = conn.recv(msg_length).decode(FORMAT)
            #############################################
            #Aqui iniciaria el protocolo                #
            #############################################
            partes = msg.split()

            # Asignamos cada campo según el orden definido
            cp_id = partes[0]
            ubicacion = partes[1]
            estado = partes[2]
            precio = float(partes[3])

            nuevo = {
                "ID": cp_id,
                "Ubicacion": ubicacion,
                "PRECIO": precio,
                "ESTADO": estado,
                # Si no vienen en el mensaje, los mantenemos si existen o ponemos None
                "CONDUCTOR_ID": None,
                "CONSUMO_KW": None,
                "IMPORTE_EU": None
            }
            #Esperamos a que se desbloquee el acceso a central_cps para poder acceder a el.
            with lock:
                central_cps[cp_id] = nuevo
            if estado == "Desconectado":
                break
            print(f"ID: {cp_id}")
            print(f"Ubicación: {ubicacion}")
            print(f"Estado: {estado}")
            print(f"Precio: {precio:.2f} €/kWh")


    conn.close()
    
def start_socket():
    #El servidor escucha:
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    #####
    #Active_count() son los objetos thread activos, es decir cada conexion
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    ##########

    #Bucle Infinito para escuchar al cliente
    while True:
        #Esperamos una conexion, conn es el socket del cliente, addr la address
        conn, addr = server.accept()
        #calculamos de nuevo los thread activos
        CONEX_ACTIVAS = threading.active_count()
        #Si no hemos sobrepasado el maximo numero de conexiones, podemos crear el thread
        if (CONEX_ACTIVAS <= MAX_CONEXIONES):
            #Creamos el Thread, target: la funcion o protocolo que atendera al cliente, args: los argumentos de la funcion 
            thread = threading.Thread(target=handle_CP, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1
        

def inicia_kafka_producer(kafka_broker):

    global kafka_producer

    print(f"[CENTRAL] Conectando a Kafka broker: {kafka_broker}")

    try:

        kafka_producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), #Formato JSON
            acks='all', #Espera confirmacion antes de considerar el mensaje como enviado
            retries=RETRIES #Reintentos si falla
        )

        print(f"[CENTRAL] Productor Kafka conectado a {kafka_broker}")
        return True

    except Exception as e:

        print(f"[ENGINE] Error conectando productor Kafka: {e}")
        return False


def inicia_kafka_consumer(kafka_broker):

    global kafka_consumer

    print(f"[CENTRAL] Conectando consumidor a Kafka: {kafka_broker}")

    try:
        kafka_consumer = KafkaConsumer(
            'solicitud-recarga',  # Topic donde los Drivers envían solicitudes
            bootstrap_servers=[kafka_broker],
            group_id='central-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')), #FFormato JSON
            auto_offset_reset='latest',
            enable_auto_commit=True,
            consumer_timeout_ms=TIMEOUT
        )
        print(f"[CENTRAL] Consumidor Kafka conectado\n")
        return True

    except Exception as e:
        print(f"[CENTRAL] Error conectando consumidor Kafka: {e}")
        return False

def validar_cp_driver(cp_id):

    with lock:
        if cp_id not in central_cps:
            return False, "CP no existe"
        
        cp=central_cps[cp_id]
        estado=cp.get("ESTADO") #????????'

        if estado=="DESCONECTADO":
            return False, "CP desconectado"
        if estado=="PARADO":
            return False, "CP parado"
        if estado=="AVERIADO":
            return False, "CP averiado"
        if estado=="SUMINISTRANDO":
            return False, "CP ocupado"
        if estado=="AUTORIZADO":
            return False, "CP reservado"
        if estado=="ACTIVADO":
            return True, "CP disponible y sin problemas"
        
        return False, f"Estado desconocido: {estado}"

def notificar_driver(driver_id, msg_type, data):

    if not kafka_producer:
        print("[CENTRAL] Productor no inicializado")
        return False
    
    try: 
        message={
            "type":msg_type,
            "timestamp":time.time(),
            **data                  
        }

        topic = f'notificaciones-{driver_id}'
        kafka_producer.send(topic, value=message)
        kafka_producer.flush()
        return True

    except Exception as e:
        print(f"[CENTRAL] Error enviando notificación: {e}")
        return False
    

def solicitud_recarga(driver_id, cp_id):

    print(f"\n[CENTRAL] Procesando olicitud de recarga:")
    print(f"            Driver: {driver_id}")
    print(f"            CP: {cp_id}")

    disp, razon=validar_cp_driver(cp_id)

    if disp:
        print(f"[CENTRAL] CP {cp_id} está disponible, iniciando autorización")

        with lock:
            central_cps[cp_id]["ESTADO"] = "AUTORIZADO"
            central_cps[cp_id]["CONDUCTOR_ID"]=driver_id

            notificar_driver(driver_id, "autorizacion_concedida", {

                "cp_id":cp_id,
                "message": "Suministro autorizado. Puede enchufarse al CP"
            })

            #Enviar orden al engine

    else:
        print(f"[CENTRAL] CP {cp_id} NO disponible: {razon}")
        
        notificar_driver(driver_id, "autorizacion_denegada", {
            "cp_id": cp_id,
            "message": razon
        })


def kafka_consumer_thread():
     
     print("[CENTRAL] A la escucha de solicitudes de Drivers\n")

     while True:
        try:
            for message in kafka_consumer:
                data=message.value
                msg_type=data.get("type")

                if msg_type == "solicitud-recarga":
                    driver_id=data.get("driver_id")
                    cp_id=data.get("cp_id")
                    solicitud_recarga(driver_id, cp_id)

        except Exception as e:
            print(f"[CENTRAL] Error en consumer thread: {e}")
            time.sleep(1)

def enviar_datos_consumo(driver_id, cp_id, consumo_kw, importe_euro):

    if not kafka_producer:
        print("[CENTRAL] Productor no inicializado")
        return False
    
    try:
        message={
            "type": "telemetria",
            "driver_id":driver_id,
            "cp_id":cp_id,
            "consumo_kw":consumo_kw,
            "importe_euro":importe_euro,
            "timestamp":time.time()
        }

        kafka_producer.send(f'datos-consumo-{driver_id}', value=message)
        return True

    except Exception as e:
        print(f"[CENTRAL] Error enviando datos de consumo: {e}")
        return False
            

def mandar_ticket(driver_id, cp_id, consumo_total, importe_total, duracion):

    print(f"\n[CENTRAL] Generando ticket final para {driver_id}")

    notificar_driver(driver_id, "suministro_finalizado", {
        "cp_id": cp_id,
        "consumo_kw": consumo_total,
        "importe_euro": importe_total,
        "duracion": duracion
    })

    with lock:
        if cp_id in central_cps:
            central_cps[cp_id]["ESTADO"] = "ACTIVADO"
            central_cps[cp_id]["CONDUCTOR_ID"] = None
            central_cps[cp_id]["CONSUMO_KW"] = 0.0
            central_cps[cp_id]["IMPORTE_EU"] = 0.0

def send_order_cp(cp_id, order_type):

    if not kafka_producer:

        print("[CENTRAL] Productor Kafka no iniciado")
        return False
    
    try:
        message = {
            "type": order_type,
            "cp_id": cp_id,
            "timestamp": time.time()
        }
        
        kafka_producer.send('cp-ordenes', value=message)
        
        print(f"[CENTRAL] Orden del tipo {order_type} enviada a CP {cp_id} por Kafka")
        
        with lock: #Actualizamos en local también
            if cp_id in central_cps:
                if order_type == "STOP":
                    central_cps[cp_id]["ESTADO"] = "PARADO"
                elif order_type == "CONTINUE":
                    central_cps[cp_id]["ESTADO"] = "ACTIVADO"
        
        return True
        
    except Exception as e:
        print(f"[CENTRAL] Error enviando orden a Kafka: {e}")
        return False




def send_order_all_cps(order_type):

    with lock:
        cp_ids = list(central_cps.keys())
    
    if not cp_ids:
        print("[CENTRAL] Warning: No hay CPs registrados")
        return
    
    print(f"\n[CENTRAL] Mandando orden {order_type} a todos los CPs: ({len(cp_ids)} CPs)")
    
    exitos = 0
    for cp_id in cp_ids:
        if send_order_cp(cp_id, order_type):
            exitos += 1

    print(f"[CENTRAL] Orden mandada con éxito a {exitos}/{len(cp_ids)} CPs\n")

######################### MAIN ##########################

if len(sys.argv) != 4:
    print("Uso correcto: python3 EV_central.py [Puerto de Escucha] [Kafka IP] [Kafka Puerto] [Kafka Broker]")
    sys.exit(1)

PORT = int(sys.argv[1])
kafka_ip = sys.argv[2]
kafka_port = int(sys.argv[3])
kafka_broker = f"{kafka_ip}:{kafka_port}"


print("[CENTRAL] - Sistema de Control EV Charging")
print("------------------------------------------------------")
print(f"Puerto de escucha: {PORT}")
print(f"Kafka Broker: {kafka_broker}")


#Creamos el servidor
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#Bindeamos el localHost y el puerto
server.bind(ADDR)

print("[STARTING] Servidor inicializándose...")
search_CP()
print("ACABO")

if not inicia_kafka_producer(kafka_broker):
    print("[CENTRAL] Error: Kafka no disponible")

if not inicia_kafka_consumer(kafka_broker):
    print("[CENTRAL] Error: Kafka no disponible")

t1 = threading.Thread(target=start_socket, daemon=True)
t1.start()
#t1.join()


if kafka_consumer:
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()


try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n[CENTRAL] Cerrando sistema")
finally:
    if kafka_producer:
        kafka_producer.close()
    if kafka_consumer:
        kafka_consumer.close()
    print("[CENTRAL] Sistema cerrado")

print("ACABO")
