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
#Asuntos (topics) mensajes: solicitud-recarga
#               notificaciones-{driver_id}
#               cp-estado
#               datos-consumo-{driver_id}
#               solicitud-cps

driver_id = None
kafka_broker = None
kafka_producer = None
kafka_consumer = None
RETRIES=3 #Reintentos kafka
TIMEOUT=5000

driver_state = {
    "current_cp": None,
    "suministro_activo": False,
    "consumo_kw": 0.0,
    "importe_euro": 0.0,
    "servicios_pendientes": []
}

def iniciar_kafka_producer(kafka_broker):

    global kafka_producer

    try:

        kafka_producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), #Formato JSON
            acks='all',
            retries=RETRIES
        )

        print(f"[DRIVER] Productor Kafka conectado a {kafka_broker}")
        return True

    except Exception as e:
        print(f"[DRIVER] Error conectando productor Kafka: {e}")
        return False

def iniciar_kafka_consumer(kafka_broker, driver_id):
     
    global kafka_consumer
    
    print(f"[DRIVER] Iniciando consumidor Kafka...")

    try:
        kafka_consumer = KafkaConsumer(
            f'notificaciones-{driver_id}', #Topics que consume
            f'datos-consumo-{driver_id}',
            'cp-estado',
            bootstrap_servers=[kafka_broker],
            group_id=f'driver-{driver_id}',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )

        print("[DRIVER] Consumidor Kafka conectado\n")
        return True
    
    except Exception as e:
        print(f"[DRIVER] Error conectando consumidor: {e}")
        return False

def solicitar_suministro(cp_id):


    if not kafka_producer:
        print("[DRIVER] Error: Productor no inicializado")
        return False

    try:

        message={

            "type": "solicitud-recarga",
            "driver_id": driver_id,
            "cp_id": cp_id,
            "timestamp": time.time()
        }

        kafka_producer.send('solicitud-recarga', value=message)
        kafka_producer.flush()
        return True

    except Exception as e:
        print(f"[DRIVER] Error enviando solicitud de recarga: {e}")
        return False

def modo_interactivo(): 
    print("\n------------------------------------")
    print("[DRIVER] Aplicación del Conductor")
    print("\n------------------------------------")
    print(f"ID del conductor: {driver_id}")
    print("\n------------------------------------")


    while True:
        print("\nOpciones:")
        print("  1. Solicitar suministro en un CP")
        print("  2. Mostrar CPs disponibles")
        print("  3. Cargar servicios desde archivo")
        print("  0. Salir")
        print("------------------------------------")

        try:
            opcion = input("\nSelecciona una opción:").strip()

            if opcion=="1":
                cp_id = input("Introduce el ID del CP: ").strip()
                print(f"[DRIVER] Solicitando recarga en el CP {cp_id}")

                if cp_id:
                    solicitar_suministro(cp_id)
                else:
                    print("[DRIVER] Id no válido")


            elif opcion=="2":
                print("\n------------------------------------")
                solicitar_lista_cps()
            elif opcion=="3":
                filepath=input("Introduzca la ruta del archivo: ").strip()
                modo_automatico(filepath)

            elif opcion=="0":
                print("[DRIVER] Saliendo de la aplicación")
                break

            else:
                print("Opción no válida")
        
        except KeyboardInterrupt:
            print("\n Programa interrumpido, cerrando aplicación")
            break

        except Exception as e:
            print(f"[DRIVER] Error: {e}")

def solicitar_lista_cps():

    if not kafka_producer:
        print("[DRIVER] Error: Productor no inicializado")
        return False
    
    try:
        message={

            "type":"solicitud-cps",
            "driver_id": driver_id,
            "timestamp": time.time()
        }
        kafka_producer.send('solicitud-cps', value=message)
        kafka_producer.flush()
        print("[DRIVER] Petición de lista de CPs enviada a Central")
        return True

    except Exception as e:
        print(f"[DRIVER] Error enviando solicitud de lista de CPs: {e}")
        return False

def modo_automatico(filepath):
    print("\n------------------------------------")
    print(f"Leyendo archivo en {filepath}")

    try:
        lista_cps=[]

        with open(filepath, 'r') as archivo:
            lineas=archivo.readlines()
            lista_cps=[linea.strip() for linea in lineas if linea.strip()]

    except Exception as e:
        print(f"[DRIVER] Error leyendo archivo: {e}")

    for cp in lista_cps:
        solicitar_suministro(cp)
        time.sleep(4)

    print("[DRIVER] Todos los CPs han sido procesados")

def kafka_consumer_thread():

    print(f"[DRICER] Iniciando hilo consumidor Kafka")

    while True:
        try:
            for message in kafka_consumer:
                handle_kafka_message(message)

        except Exception as e:
            print(f"[DRIVER] Error en hilo consumidor Kafka: {e}")

def handle_kafka_message(message):

    global driver_state

    try:
        topic=message.topic
        data=message.value
        msg_type=data.get("type")

        print(f"\n[DEBUG DRIVER] Mensaje recibido:")
        print(f"  Topic: {topic}")
        print(f"  Type: {msg_type}")
        print(f"  Data completa: {data}")
        print(f"  Driver ID en data: {data.get('driver_id')}")
        
        if topic==f"notificaciones-{driver_id}":
            
            if msg_type=="autorizacion_concedida":
                print(f"\n[DRIVER] Suministro concedido en CP {data.get('cp_id')}")


                driver_state["current_cp"] = data.get("cp_id")

            elif msg_type=="autorizacion_denegada":
                print(f"\n[DRIVER] Suministro denegado en CP {data.get('cp_id')}")

            elif msg_type == "suministro_iniciado":
                print(f"\n[DRIVER] Suministro INICIADO en CP {data.get('cp_id')}")
                driver_state["suministro_activo"] = True

            elif msg_type == "suministro_finalizado":
                print(f"\n[DRIVER] Suministro FINALIZADO")

                print(f"    ---------------------- TICKET ----------------------")
                print(f"    CP: {data.get('cp_id')}")
                print(f"    Consumo total: {data.get('consumo_kw')} kW")
                print(f"    Importe total: {data.get('importe_euro')} €")
                print(f"    Duración: {data.get('duracion')}")
                print(f"    -----------------------------------------------------\n")
                driver_state["suministro_activo"] = False
                driver_state["current_cp"] = None

            elif msg_type=="lista-cps":
                cps=data.get("cps", [])
                print(f"\n[DRIVER] Lista de CPs recibida ({len(cps)}):")
                print("  ID | Ubicación | Precio | Estado | Conductor | Consumo_kW | Importe_EU")
                print("  ---------------------------------------------------------------------")
                for cp in cps:
                    print(f"  {cp.get('ID')} | {cp.get('Ubicacion')} | {cp.get('PRECIO')} | {cp.get('ESTADO')} | {cp.get('CONDUCTOR_ID')} | {cp.get('CONSUMO_KW')} | {cp.get('IMPORTE_EU')}")
                print("  ---------------------------------------------------------------------\n")

            else:
                print(f"\n[DRIVER] Menaje no reconnocido, mensaje: {msg_type}")

        elif topic==f"datos-consumo-{driver_id}":
            print(f"Consumo: {data.get('consumo_kw')}")
            print(f"Consumo: {data.get('importe_euro')}")
            


        elif topic=="cp-estado":
            pass

        else:
            print(f"\n[DRIVER] Menaje no reconnocido, topic: {topic}")

    except Exception as e:
        print(f"[DRIVER] Error procesando mensaje: {e}")

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <KAFKA_BROKER> <DRIVER_ID> [archivo_servicios]")
        print("Ejemplo: python EV_Driver.py kafka:9092 DRIVER001")
        print("Ejemplo con archivo: python EV_Driver.py kafka:9092 DRIVER001 servicios.txt")
        sys.exit(1)
    
    kafka_broker = sys.argv[1]
    driver_id = sys.argv[2]
    if len(sys.argv)>3:
        filepath=sys.argv[3]
    else: 
        filepath=None

    print("\n--------------------------------")
    print("[DRIVER] Sistema de recarga EV")
    print("--------------------------------")
    print(f"ID del Conductor:{driver_id}")
    print(f"Kafka Broker:{kafka_broker}")
    print("--------------------------------")

    if not iniciar_kafka_producer(kafka_broker):
        sys.exit(1)
    
    if not iniciar_kafka_consumer(kafka_broker, driver_id):
        sys.exit(1)

    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    time.sleep(1)


    if filepath:
        modo_automatico(filepath)
    else:
        modo_interactivo()


    if kafka_producer:
        kafka_producer.close()
    if kafka_consumer:
        kafka_consumer.close()
    
    print("[DRIVER] Aplicación cerrada")


