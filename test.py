import sqlite3

# Conectar (esto crea el archivo test.db si no existe)
conn = sqlite3.connect("test.db")
cur = conn.cursor()

# Crear tabla
cur.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")

# Insertar un registro
cur.execute("INSERT INTO test (name) VALUES (?)", ("Juan",))

# Guardar cambios
conn.commit()

# Leer registros
cur.execute("SELECT * FROM test")
rows = cur.fetchall()
print("Contenido de la tabla:", rows)

conn.close()