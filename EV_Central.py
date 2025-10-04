import socket 
import threading
import mysql.connector

HEADER = 64
PORT = 5000
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2

central_cps = {}

def search_CP():
    conexion = mysql.connector.connect(
        host="localhost",        # Si el .py está en el mismo PC. Si está en otro, usa la IP del servidor con Docker.
        port=3307,               # El puerto que expusiste en docker-compose
        user="usuario",
        password="contraseña",
        database="database"
    )
    cursor = conexion.cursor()
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
def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")
    #############################################
    #Aqui explicariamos al cliente el protocolo #
    #############################################
    conn.send(f"Bienvenido Cliente: Este Servidor codifica tu mensaje.".encode(FORMAT))
    connected = True
    while connected:
        #El cliente envia dos mensajes, la longitud real del mensaje:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        #si hay mensaje
        if msg_length:
            msg_length = int(msg_length)
            #El mensaje real:
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
            #############################################
            #Aqui iniciaria el protocolo                #
            #############################################
            result = ""
            for char in msg:
                result += chr(ord(char) + 3)
            
            print(f" He recibido del cliente [{addr}] el mensaje: {msg}, codificado: {result}")
            conn.send(f"HOLA CLIENTE: Tu mensaje codificado es: {result} ".encode(FORMAT))
    print("ADIOS. TE ESPERO EN OTRA OCASION")
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
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1
        

######################### MAIN ##########################

#Creamos el servidor
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#Bindeamos el localHost y el puerto
server.bind(ADDR)

print("[STARTING] Servidor inicializándose...")
search_CP()


t1 = threading.Thread(target=start_socket, daemon=True)
t1.start()
