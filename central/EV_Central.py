import socket 
import threading
from threading import Lock
import mysql.connector
import os
import time
import sys
import ssl
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import logging, os
from logging.handlers import RotatingFileHandler

TLS_CERT = os.getenv("TLS_CERT", "/app/certs/certServ.pem")
TLS_ENABLED = os.getenv("TLS_ENABLED", "1") == "1"

_tls_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
_tls_ctx.load_cert_chain(TLS_CERT, TLS_CERT)

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)


logging.basicConfig(level=logging.WARNING, force=True)


log = logging.getLogger("central")
log.setLevel(logging.INFO)
log.propagate = False

fh = RotatingFileHandler(os.path.join(LOG_DIR, "central.log"),
                         maxBytes=2_000_000, backupCount=5, encoding="utf-8")
fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
fh.setFormatter(fmt)
log.addHandler(fh)


for noisy in [
    "kafka", "kafka.consumer", "kafka.producer", "kafka.conn",
    "kafka.coordinator", "kafka.client", "urllib3"
]:
    logging.getLogger(noisy).setLevel(logging.ERROR)
# Topics Kafka:
# CONSUMIDOR: solicitud-recarga (del Driver)
# # CONSUMIDOR: solicitud-cps (del Driver)
#CONSUMIDOR: cp-register (del Engine)
#CONSUMIDOR: cp-estado (del Engine)
#CONSUMIDOR: cp-telemetria (del Engine)
# PRODUCTOR: notificaciones-{driver_id} (al Driver)
# PRODUCTOR: datos-consumo-{driver_id} (telemetría al Driver)
# PRODUCTOR: f'cp-ordenes-{cp_id}' (al Engine) 
# Productor: f'autorizacion-suministro-{cp_id}' (al Engine)              

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

def send_msg_central(conn, msg):
    """Envía un mensaje al CP por el socket"""
    message = msg.encode(FORMAT)
    msg_length = len(message)
    header = f"{msg_length:<{HEADER}}".encode(FORMAT)
    conn.send(header + message)

def validar_registro_cp(cp_id):
    """
    Verifica que el CP esté registrado en CPRegistry antes de permitir autenticación
    """
    DB_HOST = os.getenv("DB_HOST", "mysql")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER", "usuario")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "contraseña")
    DB_NAME = os.getenv("DB_NAME", "database")
    
    try:
        conexion = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME
        )
        
        cursor = conexion.cursor(dictionary=True)
        cursor.execute("""
            SELECT * FROM CPRegistry 
            WHERE cp_id = %s AND registrado = 1
        """, (cp_id,))
        
        resultado = cursor.fetchone()
        cursor.close()
        conexion.close()
        
        if resultado:
            return True, resultado.get("ubicacion")
        else:
            return False, None
            
    except Exception as e:
        log.error(f"[CENTRAL] Error validando registro de CP {cp_id}: {e}")
        return False, None


def autenticar_cp(cp_id, token_registry):
    """
    Autentica un CP verificando su token de Registry y generando clave de cifrado
    """
    DB_HOST = os.getenv("DB_HOST", "mysql")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER", "usuario")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "contraseña")
    DB_NAME = os.getenv("DB_NAME", "database")
    
    try:
        conexion = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME
        )
        
        cursor = conexion.cursor(dictionary=True)
        
        # 1. Validar token en Registry
        cursor.execute("""
            SELECT * FROM CPRegistry 
            WHERE cp_id = %s AND token = %s AND registrado = 1
        """, (cp_id, token_registry))
        
        resultado = cursor.fetchone()
        
        if not resultado:
            cursor.close()
            conexion.close()
            log.warning(f"[CENTRAL] Autenticación fallida: CP {cp_id} no registrado o token inválido")
            return False, None
        
        # 2. Generar clave de cifrado única para este CP
        import secrets
        encryption_key = secrets.token_hex(16)
        
        # 3. Guardar en tabla de autenticación
        cursor.execute("""
            INSERT INTO CPAuthentication (cp_id, encryption_key, authenticated)
            VALUES (%s, %s, 1)
            ON DUPLICATE KEY UPDATE
                encryption_key = VALUES(encryption_key),
                authenticated = 1,
                fecha_auth = CURRENT_TIMESTAMP
        """, (cp_id, encryption_key))
        
        conexion.commit()
        cursor.close()
        conexion.close()
        
        log.info(f"[CENTRAL] CP {cp_id} autenticado exitosamente. Clave de cifrado generada.")
        return True, encryption_key
        
    except Exception as e:
        log.error(f"[CENTRAL] Error autenticando CP {cp_id}: {e}")
        return False, None


def revocar_clave_cp(cp_id):
    """
    Revoca la clave de cifrado de un CP específico (opción del menú)
    """
    DB_HOST = os.getenv("DB_HOST", "mysql")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER", "usuario")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "contraseña")
    DB_NAME = os.getenv("DB_NAME", "database")
    
    try:
        conexion = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME
        )
        
        cursor = conexion.cursor()
        
        cursor.execute("""
            UPDATE CPAuthentication 
            SET authenticated = 0
            WHERE cp_id = %s
        """, (cp_id,))
        
        if cursor.rowcount == 0:
            cursor.close()
            conexion.close()
            log.warning(f"[CENTRAL] CP {cp_id} no tiene autenticación para revocar")
            return False
        
        conexion.commit()
        cursor.close()
        conexion.close()
        
        log.info(f"[CENTRAL] Clave de cifrado revocada para CP {cp_id}")
        
        # Poner el CP en estado PARADO
        with lock:
            if cp_id in central_cps:
                central_cps[cp_id]["ESTADO"] = "PARADO"
        
        send_order_cp(cp_id, "STOP")
        
        return True
        
    except Exception as e:
        log.error(f"[CENTRAL] Error revocando clave de CP {cp_id}: {e}")
        return False


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
            "ESTADO": 'PARADO',
            "CONDUCTOR_ID": row["CONDUCTOR_ID"],
            "CONSUMO_KW": row["CONSUMO_KW"],
            "IMPORTE_EU": row["IMPORTE_EU"]
        }


    conexion.close()
    log.info(f"[CENTRAL] Cargados {len(central_cps)} CPs desde la BD.")

def insertar_cps_en_bd():
    DB_HOST = os.getenv("DB_HOST", "mysql")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER", "usuario")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "contraseña")
    DB_NAME = os.getenv("DB_NAME", "database")

    # 1) Conectar a la BD
    try:
        conexion = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    except mysql.connector.Error as err:
        log.error(f"[CENTRAL] No se pudo conectar a MySQL: {err}")
        return

    cursor = conexion.cursor()

    # 2) Copia segura del diccionario (evita problemas con hilos)
    with lock:
        cps_items = list(central_cps.items())

    for cp_id, cp_info in cps_items:
        try:
            # --- Datos en memoria ---
            mem_ubicacion = cp_info.get("Ubicacion")
            mem_estado = cp_info.get("ESTADO")

            # 3) Leemos la ubicación REAL que tiene la BD (para NO pisarla)
            db_ubicacion = mem_ubicacion
            cursor.execute("SELECT Ubicacion FROM ChargingPoint WHERE ID = %s", (cp_id,))
            row = cursor.fetchone()
            if row and row[0]:
                db_ubicacion = row[0]  # manda la BD

            # 4) ¿Está esa ciudad en alerta? (tabla WeatherAlert)
            alerta_meteo = 0
            if db_ubicacion:
                cursor.execute(
                    "SELECT alert_active FROM WeatherAlert WHERE location = %s",
                    (db_ubicacion,)
                )
                row_alert = cursor.fetchone()
                if row_alert is not None:
                    alerta_meteo = int(row_alert[0])

            # 5) STOP si hay alerta y procede (Central manda órdenes)
            if alerta_meteo == 1:
                # NO paramos si ya está parado/averiado/desconectado o suministrando
                if mem_estado not in ("PARADO", "AVERIADO", "DESCONECTADO", "SUMINISTRANDO"):
                    log.warning(
                        f"[CENTRAL] Meteo ALERTA en '{db_ubicacion}'. STOP a CP {cp_id} (estado={mem_estado})"
                    )
                    send_order_cp(cp_id, "STOP")

                    # reflejamos el estado en memoria
                    with lock:
                        if cp_id in central_cps:
                            central_cps[cp_id]["ESTADO"] = "PARADO"
                            mem_estado = "PARADO"

            # 6) RECOVER si NO hay alerta, pero la BD decía que estaba en alerta meteo (ALERTA_METEO=1)
            if alerta_meteo == 0:
                cursor.execute(
                    "SELECT ALERTA_METEO, ESTADO FROM ChargingPoint WHERE ID = %s",
                    (cp_id,)
                )
                row_db = cursor.fetchone()

                if row_db is not None:
                    alerta_db = int(row_db[0] or 0)
                    estado_db = row_db[1]

                    # Solo recuperamos si estaba parado POR meteo
                    if alerta_db == 1 and estado_db == "PARADO":
                        with lock:
                            estado_actual = central_cps.get(cp_id, {}).get("ESTADO")

                        # recuperamos SOLO si también lo vemos PARADO en memoria
                        if estado_actual == "PARADO":
                            log.info(
                                f"[CENTRAL] Recuperación meteo en '{db_ubicacion}'. CONTINUE a CP {cp_id}"
                            )
                            send_order_cp(cp_id, "CONTINUE")

                            with lock:
                                if cp_id in central_cps:
                                    central_cps[cp_id]["ESTADO"] = "ACTIVADO"
                                    mem_estado = "ACTIVADO"

            # 7) Insert/Update en BD
            # Ubicacion NO se pisa: NO aparece en el UPDATE
            cursor.execute("""
                INSERT INTO ChargingPoint
                    (ID, Ubicacion, PRECIO, ESTADO, CONDUCTOR_ID, CONSUMO_KW, IMPORTE_EU, ALERTA_METEO)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    PRECIO = VALUES(PRECIO),
                    ESTADO = VALUES(ESTADO),
                    CONDUCTOR_ID = VALUES(CONDUCTOR_ID),
                    CONSUMO_KW = VALUES(CONSUMO_KW),
                    IMPORTE_EU = VALUES(IMPORTE_EU),
                    ALERTA_METEO = VALUES(ALERTA_METEO)
            """, (
                cp_id,
                db_ubicacion,
                cp_info.get("PRECIO"),
                mem_estado,
                cp_info.get("CONDUCTOR_ID"),
                cp_info.get("CONSUMO_KW"),
                cp_info.get("IMPORTE_EU"),
                alerta_meteo
            ))

            conexion.commit()
            log.info(
                f"[CENTRAL] CP {cp_id} sync OK (ubicacion='{db_ubicacion}', alerta_meteo={alerta_meteo}, estado='{mem_estado}')"
            )

        except mysql.connector.Error as e:
            log.error(f"[CENTRAL] Error SQL insert/update CP {cp_id}: {e}")
        except Exception as e:
            log.error(f"[CENTRAL] Error inesperado insert/update CP {cp_id}: {e}")

    try:
        cursor.close()
    except Exception:
        pass
    try:
        conexion.close()
    except Exception:
        pass



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
            if msg.startswith("CP_AUTH:"):
                try:
                    _, cp_id, token_registry = msg.split(":", 2)
                except ValueError:
                    log.warning(f"[CENTRAL] Formato CP_AUTH inválido: {msg}")
                    response = "AUTH_FAIL:Formato inválido"
                    send_msg_central(conn, response)
                    continue
                
                # Validar que esté registrado
                registrado, ubicacion = validar_registro_cp(cp_id)
                if not registrado:
                    log.warning(f"[CENTRAL] CP {cp_id} no está registrado en Registry")
                    response = "AUTH_FAIL:No registrado"
                    send_msg_central(conn, response)
                    continue
                
                # Autenticar y generar clave
                exito, encryption_key = autenticar_cp(cp_id, token_registry)
                if exito:
                    response = f"AUTH_OK:{encryption_key}"
                    send_msg_central(conn, response)
                    log.info(f"[CENTRAL] Autenticación exitosa: {cp_id}")
                else:
                    response = "AUTH_FAIL:Token inválido"
                    send_msg_central(conn, response)
                
                continue


            #Averia
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
                        "ESTADO": "PARADO",
                        "CONDUCTOR_ID": None,
                        "CONSUMO_KW": None,
                        "IMPORTE_EU": None
                    })
                    info["ESTADO"] = "AVERIADO"
                    info["ULTIMA_AVERIA"] = motivo
                    info["TS_ULTIMO_CAMBIO"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    central_cps[cp_id] = info

                log.info(f"[CENTRAL] CP {cp_id} en AVERÍA. Motivo: {motivo}")
                continue
            #Recuperacion
            if msg.startswith("CP_RECUPERACION:"):
                try:
                    _, cp_id, motivo = msg.split(":", 2)
                except ValueError:
                    log.warning(f"[CENTRAL] Formato CP_RECUPERACION inválido: {msg}")
                    continue

                with lock:
                    info = central_cps.get(cp_id, {
                        "ID": cp_id, "Ubicacion": None, "PRECIO": None,
                        "ESTADO": "PARADO", "CONDUCTOR_ID": None,
                        "CONSUMO_KW": None, "IMPORTE_EU": None
                    })
                    # tras recuperación lo dejamos operativo (ajusta al estado que queráis)
                    info["ESTADO"] = "ACTIVADO"
                    info["ULTIMA_RECUPERACION"] = motivo
                    info["TS_ULTIMO_CAMBIO"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    central_cps[cp_id] = info

                log.info(f"[CENTRAL] CP {cp_id} RECUPERADO. Motivo: {motivo}")
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
            if estado == "DESCONECTADO":
                break
            log.info(f"ID: {cp_id}")
            log.info(f"Ubicación: {ubicacion}")
            log.info(f"Estado: {estado}")
            log.info(f"Precio: {precio:.2f} €/kWh")


    conn.close()
def start_socket():
    # El servidor escucha:
    server.listen()
    log.info(f"[LISTENING] Servidor a la escucha en {SERVER}")

    # Active_count() son los objetos thread activos (cada conexion)
    CONEX_ACTIVAS = threading.active_count() - 1
    log.info(CONEX_ACTIVAS)

    while True:
        # Aceptamos conexión TCP (raw)
        conn, addr = server.accept()

        # --- Envolver con TLS (mTLS) si está activado ---
        if TLS_ENABLED:
            try:
                conn = _tls_ctx.wrap_socket(conn, server_side=True)
            except ssl.SSLError as e:
                log.warning(f"[CENTRAL] Handshake TLS fallido desde {addr}: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
                continue

        # Recalculamos threads activos
        CONEX_ACTIVAS = threading.active_count()

        # Si no hemos sobrepasado el máximo número de conexiones, creamos el thread
        if CONEX_ACTIVAS <= MAX_CONEXIONES:
            thread = threading.Thread(target=handle_CP, args=(conn, addr), daemon=True)
            thread.start()
            log.info(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            log.info(f"CONEXIONES RESTANTES PARA CERRAR EL SERVICIO {MAX_CONEXIONES - CONEX_ACTIVAS}")
        else:
            log.warning("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            try:
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass


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
                'solicitud-cps',
                'cp-register',
                'cp-telemetria',
                'cp-estado',  # Topic donde los Drivers envían solicitudes
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
            "type": "AUTORIZADO",
            "cp_id": cp_id,
            "conductor_id": driver_id,
            "timestamp": time.time()
        }
        
        kafka_producer.send(f'autorizacion-suministro-{cp_id}', value=message)
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

                    with lock:
                        if cp_id in central_cps:
                            estado_anterior = central_cps[cp_id]["ESTADO"]
                            nuevo_estado = data.get("status")
                            central_cps[cp_id]["ESTADO"] = nuevo_estado
                            
                            print(f"[CENTRAL] Estado de CP {cp_id} cambiado: {estado_anterior} -> {nuevo_estado}")
                        else:
                            print(f"[CENTRAL] Recibido estado de CP desconocido: {cp_id}")

                    if msg_type=="suministro_finalizado":
                        conductor_id = data.get("conductor_id")
                        consumo_kw = data.get("consumo_total")
                        importe_euro = data.get("importe_total")
                        duracion = data.get("duracion")
                        mandar_ticket(cp_id, conductor_id, consumo_kw, importe_euro, duracion)

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
        
        kafka_producer.send(f'cp-ordenes-{cp_id}', value=message)
        kafka_producer.flush()
        
        log.info(f"[CENTRAL] Orden del tipo {order_type} enviada a CP {cp_id} por Kafka")

        conductor_afectado = None
        with lock:
            if cp_id in central_cps:
                conductor_afectado = central_cps[cp_id].get("CONDUCTOR_ID")
        
        if order_type == "STOP" and conductor_afectado:
            log.info(f"[CENTRAL] Notificando a conductor {conductor_afectado} que su CP fue parado")
            notificar_driver(conductor_afectado, "cp_parado", {
                "cp_id": cp_id,
                "message": "El CP fue puesto fuera de servicio por la Central"
            })
        
            with lock:
                if cp_id in central_cps:
                    central_cps[cp_id]["CONDUCTOR_ID"] = None
        
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
        print("   PANEL DE CONTROL EV_CENTRAL ")
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
        elif opcion == "4": 
            menu_revocar_clave()
        elif opcion == "0":
            print("[CENTRAL] Opción 0 seleccionada. Saliendo...")
            break # Rompe el bucle del menú para apagar
        else:
            print(f"[ERROR] Opción '{opcion}' no válida. Inténtalo de nuevo.")

def menu_revocar_clave():
    """Menú para revocar la clave de cifrado de un CP"""
    cp_id = input("  Introduce el ID del CP: ").strip()
    
    confirmacion = input(f"  ¿Confirmas revocar la clave de {cp_id}? (s/n): ").strip().lower()
    if confirmacion == "s":
        if revocar_clave_cp(cp_id):
            print(f"  ✓ Clave revocada. {cp_id} deberá autenticarse de nuevo.")
        else:
            print(f"  ✗ No se pudo revocar la clave.")
    else:
        print("  Operación cancelada.")

def sync_cps_periodicamente(intervalo_segundos=5):

    log.info(f"[CENTRAL] Hilo de sincronización BD iniciado (cada {intervalo_segundos}s).")
    while True:
        try:
            insertar_cps_en_bd()
        except Exception as e:
            log.error(f"[CENTRAL] Error en sincronización periódica con BD: {e}")
        time.sleep(intervalo_segundos)


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

sync_thread = threading.Thread(target=sync_cps_periodicamente, args=(5,), daemon=True)
sync_thread.start()

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
insertar_cps_en_bd()