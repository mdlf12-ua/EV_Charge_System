import os
import time
import threading
import requests
import logging
from logging.handlers import RotatingFileHandler

# -------- LOGGING SOLO A FICHERO -------- #

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log = logging.getLogger("EV_W")
log.setLevel(logging.INFO)
log.propagate = False  # no propagar al root

fh = RotatingFileHandler(
    os.path.join(LOG_DIR, "ev_w.log"),
    maxBytes=2_000_000,
    backupCount=5,
    encoding="utf-8"
)
fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
fh.setFormatter(fmt)
log.addHandler(fh)

# ----------------- CONFIG ----------------- #

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not OPENWEATHER_API_KEY:
    raise RuntimeError("Falta OPENWEATHER_API_KEY en las variables de entorno")

# URL base de OpenWeather (tiempo actual)
OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"

# URL base de tu API_Central
# Dentro de docker-compose, el host será 'api_central'
API_CENTRAL_BASE = os.getenv("API_CENTRAL_BASE", "https://api-central:8000")

# Intervalo entre consultas (segundos)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 4))

# ----------------- ESTADO COMPARTIDO ----------------- #

locations = set()          # conjunto de ciudades a vigilar
alert_state = {}           # ciudad -> bool (True si en alerta)
lock = threading.Lock()    # protege locations y alert_state
running = True             # bandera para parar hilos limpiamente


# ----------------- FUNCIONES AUXILIARES ----------------- #

CA_CERT = os.getenv("CA_CERT", "/app/certs/certificado_CA.crt")

def get_temperature(city: str):
    """
    Devuelve la temperatura actual en ºC para una ciudad usando OpenWeather.
    """
    params = {
        "q": city,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }
    try:
        resp = requests.get(OPENWEATHER_URL, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        temp = data["main"]["temp"]  # ºC
        return float(temp)
    except Exception as e:
        log.error(f"[EV_W] Error obteniendo temperatura para {city}: {e}")
        return None


def notify_alert(location: str, temp: float):
    """
    Llama al API_Central para registrar una alerta de clima.
    """
    url = f"{API_CENTRAL_BASE}/weather/alert"
    payload = {
        "location": location,
        "temperature": temp
    }
    try:
        resp = requests.post(url, json=payload, timeout=5, verify=CA_CERT)
        if resp.status_code == 200:
            log.info(f"[EV_W] ALERTA enviada a API_Central para {location} (T={temp:.2f} ºC)")
        else:
            log.warning(f"[EV_W] Error HTTP {resp.status_code} en alert {location}: {resp.text}")
    except Exception as e:
        log.error(f"[EV_W] Error conectando con API_Central (alert) para {location}: {e}")


def notify_recover(location: str, temp: float):
    """
    Llama al API_Central para registrar la recuperación de la alerta.
    """
    url = f"{API_CENTRAL_BASE}/weather/recover"
    payload = {
        "location": location,
        "temperature": temp
    }
    try:
        resp = requests.post(url, json=payload, timeout=5, verify=CA_CERT)
        if resp.status_code == 200:
            log.info(f"[EV_W] RECUPERACIÓN enviada a API_Central para {location} (T={temp:.2f} ºC)")
        else:
            log.warning(f"[EV_W] Error HTTP {resp.status_code} en recover {location}: {resp.text}")
    except Exception as e:
        log.error(f"[EV_W] Error conectando con API_Central (recover) para {location}: {e}")


# ----------------- HILO METEOROLÓGICO ----------------- #

def weather_loop():
    """
    Hilo que cada POLL_INTERVAL segundos consulta OpenWeather para
    cada localización registrada y envía alertas/recuperaciones al API_Central.
    """
    global running

    log.info("[EV_W] Hilo meteorológico arrancado. Intervalo: %s s", POLL_INTERVAL)

    while running:
        with lock:
            cities = list(locations)

        if not cities:
            log.info("[EV_W] No hay localizaciones registradas. Esperando...")
            time.sleep(POLL_INTERVAL)
            continue

        for city in cities:
            if not running:
                break

            temp = get_temperature(city)
            if temp is None:
                continue  # error al obtener temperatura

            with lock:
                en_alerta = alert_state.get(city, False)

            log.info(f"[EV_W] Clima en {city}: {temp:.2f} ºC (alerta={en_alerta})")

            if temp < 0.0 and not en_alerta:
                # Pasamos de OK -> ALERTA
                notify_alert(city, temp)
                with lock:
                    alert_state[city] = True

            elif temp >= 0.0 and en_alerta:
                # Pasamos de ALERTA -> OK
                notify_recover(city, temp)
                with lock:
                    alert_state[city] = False

        time.sleep(POLL_INTERVAL)

    log.info("[EV_W] Hilo meteorológico detenido.")

def change_cp_location(cp_id: str, new_location: str):
    """
    Pide a API_Central que cambie la ciudad de un CP en la BD.
    """
    url = f"{API_CENTRAL_BASE}/cps/{cp_id}/location"
    payload = {"location": new_location}

    try:
        resp = requests.post(url, json=payload, timeout=5, verify=CA_CERT)
        if resp.status_code == 200:
            log.info(f"[EV_W] CP {cp_id} cambiado a ciudad '{new_location}' via API_Central")
            print(f"  [OK] CP {cp_id} ahora está en '{new_location}'")
        else:
            log.warning(f"[EV_W] Error HTTP {resp.status_code} cambiando CP {cp_id}: {resp.text}")
            print(f"  [ERROR] HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        log.error(f"[EV_W] Error conectando con API_Central para cambiar CP {cp_id}: {e}")
        print(f"  [ERROR] No se pudo contactar con API_Central: {e}")

# ----------------- MENÚ INTERACTIVO ----------------- #

def mostrar_menu():
    print("\n============================")
    print("   WEATHER CONTROL (EV_W)   ")
    print("============================")
    print(" 1. Listar localizaciones")
    print(" 2. Añadir localización")
    print(" 3. Eliminar localización")
    print(" 4. Vaciar todas las localizaciones")
    print(" 5. Cambiar ciudad de un CP (Ubicacion en BD)")
    print(" 0. Salir")
    print("============================")


def menu_loop():
    """
    Menú en primer plano: permite añadir/quitar localizaciones
    sin parar el hilo meteorológico.
    """
    global running

    while True:
        mostrar_menu()
        opcion = input(" Elige una opción: ").strip()

        if opcion == "1":
            with lock:
                if not locations:
                    print(" [EV_W] No hay localizaciones registradas.")
                else:
                    print(" Localizaciones actuales:")
                    for loc in locations:
                        estado = "ALERTA" if alert_state.get(loc, False) else "OK"
                        print(f"  - {loc} ({estado})")

        elif opcion == "2":
            loc = input(" Introduce el nombre de la ciudad/localización: ").strip()
            if not loc:
                print("  [ERROR] Localización vacía.")
                continue
            with lock:
                locations.add(loc)
                alert_state.setdefault(loc, False)
            print(f"  [EV_W] Localización '{loc}' añadida.")

        elif opcion == "3":
            loc = input(" Introduce la ciudad a eliminar: ").strip()
            with lock:
                if loc in locations:
                    locations.remove(loc)
                    alert_state.pop(loc, None)
                    print(f"  [EV_W] Localización '{loc}' eliminada.")
                else:
                    print(f"  [ERROR] '{loc}' no está en la lista.")

        elif opcion == "4":
            confirm = input(" ¿Seguro que quieres borrar TODAS las localizaciones? (s/n): ").strip().lower()
            if confirm == "s":
                with lock:
                    locations.clear()
                    alert_state.clear()
                print("  [EV_W] Todas las localizaciones han sido eliminadas.")
        elif opcion == "5":
            cp_id = input(" ID del CP (ej: CP001): ").strip()
            if not cp_id:
                print("  [ERROR] CP ID vacío.")
                continue

            new_loc = input(" Nueva ciudad/localización: ").strip()
            if not new_loc:
                print("  [ERROR] Localización vacía.")
                continue

            change_cp_location(cp_id, new_loc)

        elif opcion == "0":
            print("  [EV_W] Saliendo de EV_W...")
            running = False
            break

        else:
            print("  [ERROR] Opción no válida. Inténtalo de nuevo.")


# ----------------- MAIN ----------------- #

def main():
    # Arrancamos hilo meteorológico en background
    hilo_meteo = threading.Thread(target=weather_loop, daemon=True)
    hilo_meteo.start()

    # Menú corre en el hilo principal (para poder usar input)
    try:
        menu_loop()
    except KeyboardInterrupt:
        print("\n[EV_W] Interrumpido por el usuario.")
    finally:
        global running
        running = False
        time.sleep(1)
        log.info("[EV_W] EV_W detenido.")


if __name__ == "__main__":
    main()
