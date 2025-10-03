import mysql.connector

# Conectar a la base de datos
conexion = mysql.connector.connect(
    host="localhost",        # Si el .py está en el mismo PC. Si está en otro, usa la IP del servidor con Docker.
    port=3307,               # El puerto que expusiste en docker-compose
    user="usuario",
    password="contraseña",
    database="database"
)

cursor = conexion.cursor()

# Crear tabla (si no existe)
cursor.execute("""
CREATE TABLE IF NOT EXISTS clientes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL,
    saldo DECIMAL(10,2) NOT NULL
)
""")

# Insertar datos
cursor.execute("INSERT INTO clientes (nombre, saldo) VALUES (%s, %s)", ("Ana", 1000.50))
cursor.execute("INSERT INTO clientes (nombre, saldo) VALUES (%s, %s)", ("Luis", 250.00))

conexion.commit()  # Muy importante para guardar cambios

# Leer datos
cursor.execute("SELECT * FROM clientes")
for fila in cursor.fetchall():
    print(fila)

# Cerrar
cursor.close()
conexion.close()
