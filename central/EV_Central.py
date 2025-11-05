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
import logging, os

# Carpeta de logs (persistida con un volumen)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(LOG_DIR, "central.log"),
            maxBytes=2_000_000,     # 2 MB por archivo
            backupCount=5,          # guarda 5 rotaciones
            encoding="utf-8"
        )
        # Ojo: NO añadimos StreamHandler. Así los logs NO van a la consola.
    ],
    force=True  # pisa cualquier config previa
)
for noisy in ["kafka", "kafka.consumer", "kafka.producer", "kafka.conn", "urllib3"]:
    logging.getLogger(noisy).setLevel(logging.WARNING)

log = logging.getLogger("central")
# Topics Kafka:
# CONSUMIDOR: solicitud-recarga (del Driver)
# # CONSUMIDOR: solicitud-cps (del Driver)
#CONSUMIDOR: cp-register (del Engine)
#CONSUMIDOR: cp-estado (del Engine)
#CONSUMIDOR: cp-telemetria (del Engine)
# PRODUCTOR: notificaciones-{driver_id} (al Driver)
# PRODUCTOR: datos-consumo-{driver_id} (telemetría al Driver)
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
            log.info("Conectado a MySQL")
            break
        except mysql.connector.Error as err:
            log.warning(f"No se pudo conectar a MySQL, reintentando en 5 segundos... ({err})")
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
    log.info(f"[CENTRAL] Cargados {len(central_cps)} CPs desde la BD.")


#Función que utilizara cada hilo para antender a un cliente
#Función que utilizara cada hilo para antender a un cliente
def handle_CP(conn, addr):
    log.info(f"[NUEVA CONEXION] {addr} connected.")
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
            if msg.startswith("CP_AVERIA:"):
                try:
                    _, cp_id, motivo = msg.split(":", 2)
                except ValueError:
                    log.warning(f"[CENTRAL] Formato CP_AVERIA inválido: {msg}")
                    continue

                with lock:
                    # asegurar que existe el CP en la tabla interna
                    info = central_cps.get(cp_id, {
                        "ID": cp_id,
                        "Ubicacion": None,
                        "PRECIO": None,
                        "ESTADO": "Parado",
                        "CONDUCTOR_ID": None,
                        "CONSUMO_KW": None,
                        "IMPORTE_EU": None
                    })
                    info["ESTADO"] = "Averiado"
                    info["ULTIMA_AVERIA"] = motivo
                    info["TS_ULTIMO_CAMBIO"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    central_cps[cp_id] = info

                log.info(f"[CENTRAL] CP {cp_id} en AVERÍA. Motivo: {motivo}")
                continue

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
            log.info(f"ID: {cp_id}")
            log.info(f"Ubicación: {ubicacion}")
            log.info(f"Estado: {estado}")
            log.info(f"Precio: {precio:.2f} €/kWh")


    conn.close()
    
def start_socket():
    #El servidor escucha:
    server.listen()
    log.info(f"[LISTENING] Servidor a la escucha en {SERVER}")
    #####
    #Active_count() son los objetos thread activos, es decir cada conexion
    CONEX_ACTIVAS = threading.active_count()-1
    log.info(CONEX_ACTIVAS)
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
            log.info(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            log.info("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            log.warning("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1
        

def inicia_kafka_producer(kafka_broker,max_retries=10, delay=5):

    global kafka_producer

    log.info(f"[CENTRAL] Conectando a Kafka broker: {kafka_broker}")
    for intento in range(1, max_retries + 1):
        try:

            kafka_producer = KafkaProducer(
                bootstrap_servers=[kafka_broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'), #Formato JSON
                acks='all', #Espera confirmacion antes de considerar el mensaje como enviado
                retries=RETRIES #Reintentos si falla
            )

            log.info(f"[CENTRAL] Productor Kafka conectado a {kafka_broker}")
            return True

        except Exception as e:

            log.warning(f"[ENGINE] Error conectando productor Kafka: {e}")
            if intento < max_retries:
                log.info(f"[CENTRAL] Reintentando en {delay} segundos...")
                time.sleep(delay)
            else:
                log.warning("[CENTRAL] No se pudo conectar a Kafka después de varios intentos.")
                return False
            return False


def inicia_kafka_consumer(kafka_broker,max_retries=10, delay=5):

    global kafka_consumer

    log.info(f"[CENTRAL] Conectando consumidor a Kafka: {kafka_broker}")
    for intento in range(1, max_retries + 1):
        try:
            kafka_consumer = KafkaConsumer(
                'solicitud-recarga',
                'solicitud-cps',  # Topic donde los Drivers envían solicitudes
                bootstrap_servers=[kafka_broker],
                group_id='central-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')), #FFormato JSON
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=TIMEOUT
            )
            log.info(f"[CENTRAL] Consumidor Kafka conectado\n")
            return True

        except Exception as e:
            log.warning(f"[CENTRAL] Error conectando consumidor Kafka: {e}")
            if intento < max_retries:
                log.info(f"[CENTRAL] Reintentando en {delay} segundos...")
                time.sleep(delay)
            else:
                log.warning("[CENTRAL] No se pudo conectar a Kafka después de varios intentos.")
                return False
            return False

def enviar_lista_cps(driver_id):

    if not kafka_producer:
        log.warning("[CENTRAL] Productor no inicializado, no se puede enviar lista de CPs")
        return False
    
    with lock:

        cps_list=[]
        for cp_id, cp in central_cps.items():
            cps_list.append({
                "ID": cp.get("ID"),
                "Ubicacion": cp.get("Ubicacion"),
                "PRECIO": cp.get("PRECIO"),
                "ESTADO": cp.get("ESTADO"),
                "CONDUCTOR_ID": cp.get("CONDUCTOR_ID"),
                "CONSUMO_KW": cp.get("CONSUMO_KW"),
                "IMPORTE_EU": cp.get("IMPORTE_EU")
            })
    try:
        notificar_driver(driver_id, "lista-cps", {"cps":cps_list})
        log.info(f"[CENTRAL] Enviada lista de {len(cps_list)} CPs a driver {driver_id}")
        return True
    
    except Exception as e:
        log.warning(f"[CENTRAL] Error enviando lista de CPs a driver {driver_id}: {e}")
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
        if estado=="OK":
            return True, "CP disponible y sin problemas"
        
        return False, f"Estado desconocido: {estado}"

def notificar_driver(driver_id, msg_type, data):

    if not kafka_producer:
        log.warning("[CENTRAL] Productor no inicializado")
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
        log.warning(f"[CENTRAL] Error enviando notificación: {e}")
        return False
    

def solicitud_recarga(driver_id, cp_id):

    log.info(f"\n[CENTRAL] Procesando olicitud de recarga:")
    log.info(f"            Driver: {driver_id}")
    log.info(f"            CP: {cp_id}")

    disp, razon=validar_cp_driver(cp_id)

    if disp:
        log.info(f"[CENTRAL] CP {cp_id} está disponible, iniciando autorización")

        with lock:
            central_cps[cp_id]["ESTADO"] = "AUTORIZADO"
            central_cps[cp_id]["CONDUCTOR_ID"]=driver_id

            notificar_driver(driver_id, "autorizacion_concedida", {

                "cp_id":cp_id,
                "message": "Suministro autorizado. Puede enchufarse al CP"
            })
            send_autorizacion_engine(cp_id, driver_id)
            
            

    else:
        log.warning(f"[CENTRAL] CP {cp_id} NO disponible: {razon}")
        
        notificar_driver(driver_id, "autorizacion_denegada", {
            "cp_id": cp_id,
            "message": razon
        })


def send_autorizacion_engine(cp_id, driver_id):
    if not kafka_producer:
        print("[CENTRAL] Productor Kafka no inicializado")
        return False
    
    try:
        message = {
            "type": "autorizado",
            "cp_id": cp_id,
            "conductor_id": driver_id,
            "timestamp": time.time()
        }
        
        kafka_producer.send('autorizacion-suministro', value=message)
        kafka_producer.flush()
        
        print(f"[CENTRAL] Autorización enviada a Engine {cp_id}")
        return True
        
    except Exception as e:
        print(f"[CENTRAL] Error enviando autorización: {e}")
        return False

def kafka_consumer_thread():
     
     log.info("[CENTRAL] A la escucha de solicitudes de Drivers\n")

     while True:
        try:
            for message in kafka_consumer:
                data=message.value
                topic = message.topic 
                msg_type=data.get("type")

                if topic == "solicitud-recarga":
                    driver_id=data.get("driver_id")
                    cp_id=data.get("cp_id")
                    solicitud_recarga(driver_id, cp_id)


                elif topic == "solicitud-cps":
                    driver_id = data.get("driver_id")
                    if driver_id:
                        enviar_lista_cps(driver_id)
                        log.info("[CENTRAL] Lista CPs enviada")
                    else:
                        log.warning("[CENTRAL] solicitud-lista-cps sin driver_id valida")

                elif topic=='cp-register':
                    cp_id=data.get("cp_id")
                    ubicacion=data.get("ubicacion")
                    precio_kwh = data.get("precio_kwh")
                    status = data.get("status")
                    consumo_kw=data.get("consumo_kw")
                    importe_eu=data.get("importe_euro")
                    print(f"\n[CENTRAL] Nuevo CP registrado:")
                    print(f"            ID: {cp_id}")
                    print(f"            Ubicación: {ubicacion}")
                    print(f"            Precio: {precio_kwh} €/kWh")
                    print(f"            Estado: {status}\n")
                
                    with lock:
                        central_cps[cp_id] = {
                            "ID": cp_id,
                            "Ubicacion": ubicacion,
                            "PRECIO": precio_kwh,
                            "ESTADO": status,
                            "CONDUCTOR_ID": None,
                            "CONSUMO_KW": consumo_kw,
                            "IMPORTE_EU": importe_eu
                        }

                elif topic=="cp-estado":
                    cp_id=data.get("cp_id")

                    if msg_type=="suministro_finalizado":
                        conductor_id = data.get("conductor_id")
                        consumo_kw = data.get("consumo_total")
                        importe_euro = data.get("importe_total")
                        duracion = data.get("duracion")
                        mandar_ticket(cp_id, conductor_id, consumo_kw, importe_euro, duracion)

                    print(f"[CENTRAL] Estado cambiado en Engine de {central_cps[cp_id]["ESTADO"]} a {data.get("status")} con éxito")
                    with lock:
                            if cp_id in central_cps:
                                status = data.get("status")
                                central_cps[cp_id]["ESTADO"] = status

                elif topic=="cp-telemetria":
                    cp_id = data.get("cp_id")
                    conductor_id = data.get("conductor_id")
                    consumo_kw = data.get("consumo_kw")
                    importe_euro = data.get("importe_euro")

                    with lock:
                        if cp_id in central_cps:
                            central_cps[cp_id]["CONSUMO_KW"] = consumo_kw
                            central_cps[cp_id]["IMPORTE_EU"] = importe_euro
                        
                        enviar_datos_consumo(conductor_id, cp_id, consumo_kw, importe_euro)

                else:
                    print(f"[CENTRAL] Topic no reconocido: {topic}")


        except Exception as e:
            log.error(f"[CENTRAL] Error en consumer thread: {e}")
            time.sleep(1)




def enviar_datos_consumo(driver_id, cp_id, consumo_kw, importe_euro):

    if not kafka_producer:
        log.error("[CENTRAL] Productor no inicializado")
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
        log.error(f"[CENTRAL] Error enviando datos de consumo: {e}")
        return False
            

def mandar_ticket( cp_id, driver_id, consumo_total, importe_total, duracion):

    log.info(f"\n[CENTRAL] Generando ticket final para {driver_id}")

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

        log.error("[CENTRAL] Productor Kafka no iniciado")
        return False
    
    try:
        message = {
            "type": order_type,
            "cp_id": cp_id,
            "timestamp": time.time()
        }
        
        kafka_producer.send('cp-ordenes', value=message)
        
        log.info(f"[CENTRAL] Orden del tipo {order_type} enviada a CP {cp_id} por Kafka")
        
        with lock: #Actualizamos en local también
            if cp_id in central_cps:
                if order_type == "STOP":
                    central_cps[cp_id]["ESTADO"] = "PARADO"
                elif order_type == "CONTINUE":
                    central_cps[cp_id]["ESTADO"] = "ACTIVADO"
        
        return True
        
    except Exception as e:
        log.error(f"[CENTRAL] Error enviando orden a Kafka: {e}")
        return False




def send_order_all_cps(order_type):

    with lock:
        cp_ids = list(central_cps.keys())
    
    if not cp_ids:
        log.warning("[CENTRAL] Warning: No hay CPs registrados")
        return
    
    log.info(f"\n[CENTRAL] Mandando orden {order_type} a todos los CPs: ({len(cp_ids)} CPs)")
    
    exitos = 0
    for cp_id in cp_ids:
        if send_order_cp(cp_id, order_type):
            exitos += 1

    log.info(f"[CENTRAL] Orden mandada con éxito a {exitos}/{len(cp_ids)} CPs\n")

def show_cp_status():
    """
    Muestra el estado actual de todos los CPs registrados en la central.
    Inspirado en el "MONITORIZATION PANEL" del PDF [cite: 75-100].
    """
    print("\n---  MONITORIZACIÓN DE CHARGING POINTS ---")
    
    # Usamos el lock para asegurar una lectura segura del diccionario
    with lock:
        if not central_cps:
            print(" [CENTRAL] No hay Charging Points registrados o conectados.")
            print("---------------------------------------------")
            return

        # Ordenamos por ID para una visualización consistente
        sorted_cp_ids = sorted(central_cps.keys())
        
        for cp_id in sorted_cp_ids:
            cp = central_cps[cp_id]
            
            # Obtenemos valores con 'get' para evitar errores si una clave falta
            estado = cp.get("ESTADO", "DESCONOCIDO")
            ubicacion = cp.get("Ubicacion", "N/A")
            precio = cp.get("PRECIO", 0.0)
            
            print(f"\n [CP ID]: {cp_id} ({ubicacion})")
            print(f"   Precio: {precio} €/kWh")
            
            # Damos formato al estado según el PDF
            if estado == "ACTIVADO":
                print(f"   Estado: {estado} (VERDE - Disponible)")
            elif estado == "SUMINISTRANDO":
                conductor = cp.get("CONDUCTOR_ID", "N/A")
                consumo = cp.get("CONSUMO_KW", 0.0)
                importe = cp.get("IMPORTE_EU", 0.0)
                print(f"   Estado: {estado} (VERDE - Ocupado)")
                print(f"     > Conductor: {conductor}")
                print(f"     > Consumo: {consumo:.2f} kWh")
                print(f"     > Importe: {importe:.2f} €")
            elif estado == "PARADO":
                print(f"   Estado: {estado} (NARANJA - Out of Order)") # [cite: 84, 85]
            elif estado == "AVERIADO":
                print(f"   Estado: {estado} (ROJO - Averiado)") # [cite: 98, 99]
            elif estado == "DESCONECTADO":
                print(f"   Estado: {estado} (GRIS - Desconectado)") # [cite: 100, 168]
            else:
                 print(f"   Estado: {estado} (Desconocido)")

    print("\n---------------------------------------------")


def menu_send_order_one():
    """
    Menú para enviar una orden (STOP/CONTINUE) a un CP específico.
    """
    cp_id = input("  Introduce el ID del CP: ").strip()
    
    # Verificamos que el CP existe antes de enviar la orden
    with lock:
        if cp_id not in central_cps:
            print(f"  [ERROR] CP ID '{cp_id}' no encontrado.")
            return

    print(f"  ¿Qué orden quieres enviar a {cp_id}?")
    print("    1. Parar (Poner Fuera de Servicio)")
    print("    2. Reanudar (Poner en Activado)")
    orden = input("  Elige (1-2): ").strip()

    if orden == "1":
        print(f"  Enviando STOP a {cp_id}...")
        # Esta es tu función existente
        send_order_cp(cp_id, "STOP")
    elif orden == "2":
        print(f"  Enviando CONTINUE a {cp_id}...")
        # Esta es tu función existente
        send_order_cp(cp_id, "CONTINUE")
    else:
        print("  Opción no válida.")


def menu_send_order_all():
    """
    Menú para enviar una orden (STOP/CONTINUE) a TODOS los CPs.
    """
    print(f"  ¿Qué orden quieres enviar a TODOS los CPs?")
    print("    1. Parar (Poner Fuera de Servicio)")
    print("    2. Reanudar (Poner en Activado)")
    orden = input("  Elige (1-2): ").strip()

    if orden == "1":
        print(f"  Enviando STOP a TODOS los CPs...")
        # Esta es tu función existente
        send_order_all_cps("STOP")
    elif orden == "2":
        print(f"  Enviando CONTINUE a TODOS los CPs...")
        # Esta es tu función existente
        send_order_all_cps("CONTINUE")
    else:
        print("  Opción no válida.")


def central_menu():
    """
    Bucle principal del menú interactivo de la Central.
    """
    while True:
        print("\n=====================================")
        print(" 🏛️  PANEL DE CONTROL EV_CENTRAL 🏛️")
        print("=====================================")
        print(" 1. Mostrar estado de todos los CPs")
        print(" 2. Enviar orden (Parar/Reanudar) a un CP")
        print(" 3. Enviar orden (Parar/Reanudar) a TODOS los CPs")
        print(" 0. Salir (Apagar Central)")
        print("=====================================")
        
        opcion = input(" Selecciona una opción: ").strip()

        if opcion == "1":
            show_cp_status()
        elif opcion == "2":
            menu_send_order_one()
        elif opcion == "3":
            menu_send_order_all()
        elif opcion == "0":
            print("[CENTRAL] Opción 0 seleccionada. Saliendo...")
            break # Rompe el bucle del menú para apagar
        else:
            print(f"[ERROR] Opción '{opcion}' no válida. Inténtalo de nuevo.")



######################### MAIN ##########################


if len(sys.argv) != 4:
    log.warning("Uso correcto: python3 EV_central.py [Puerto de Escucha] [Kafka IP] [Kafka Puerto] [Kafka Broker]")
    sys.exit(1)
    
PORT = int(sys.argv[1])
kafka_ip = sys.argv[2]
kafka_port = int(sys.argv[3])
kafka_broker = f"{kafka_ip}:{kafka_port}"


log.info("[CENTRAL] - Sistema de Control EV Charging")
log.info("------------------------------------------------------")
log.info(f"Puerto de escucha: {PORT}")
log.info(f"Kafka Broker: {kafka_broker}")


#Creamos el servidor
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#Bindeamos el localHost y el puerto
server.bind(ADDR)

log.info("[STARTING] Servidor inicializándose...")
search_CP()
log.info("ACABO")

if not inicia_kafka_producer(kafka_broker):
    log.error("[CENTRAL] Error: Kafka no disponible")

if not inicia_kafka_consumer(kafka_broker):
    log.error("[CENTRAL] Error: Kafka no disponible")

t1 = threading.Thread(target=start_socket, daemon=True)
t1.start()
#t1.join()


if kafka_consumer:
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()


try:
    central_menu()

except KeyboardInterrupt:
    log.info("\n[CENTRAL] Cerrando sistema")
finally:
    if kafka_producer:
        kafka_producer.close()
    if kafka_consumer:
        kafka_consumer.close()
    log.info("[CENTRAL] Sistema cerrado")

log.info("ACABO")
