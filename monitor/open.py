#!/usr/bin/env python3
import subprocess
import sys

DOCKER_COMPOSE_FILE = "docker-compose.yml"

def abrir_monitor():
    mon = input("Número del monitor que quieres abrir: ").strip()
    nombre_servicio = f"monitor{mon}" if mon else "monitor"
    subprocess.run(["docker", "compose", "up", "-d", nombre_servicio])

def abrir_engine():
    mon = input("Número del engine que quieres abrir: ").strip()
    nombre_servicio = f"engine{mon}" if mon else "engine"
    subprocess.run(["docker", "compose", "up", "-d", nombre_servicio])

def abrir_todos():
   subprocess.run(["docker", "compose", "up", "-d"])

def borrar_monitor():
    mon = input("Número del monitor que quieres abrir: ").strip()
    nombre_servicio = f"monitor{mon}" if mon else "monitor"
    subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "stop", nombre_servicio])
    subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "rm", "-f", nombre_servicio])

def borrar_engine():
    mon = input("Número del engine que quieres abrir: ").strip()
    nombre_servicio = f"engine{mon}" if mon else "engine"
    subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "stop", nombre_servicio])
    subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "rm", "-f", nombre_servicio])
def borrar_todo():
    subprocess.run(["docker", "compose", "down", "-v", "--rmi", "all"])

def menu():
    while True:
        print("\n=================================")
        print("  GESTOR DE CONTENEDORES DOCKER  ")
        print("=================================")
        print("1 - Abrir un MONITOR")
        print("2 - Abrir un ENGINE")
        print("3 - Abrir TODOS")
        print("4 - Borrar MONITOR")
        print("5 - Borrar ENGINE")
        print("6 - Borrar TODO")
        print("0 - Salir")
        print("=================================")

        opcion = input("Selecciona una opción: ").strip()

        if opcion == "1":
            abrir_monitor()
        elif opcion == "2":
            abrir_engine()
        elif opcion == "3":
            abrir_todos()
        elif opcion == "4":
            borrar_monitor()
        elif opcion == "5":
            borrar_engine()
        elif opcion == "6":
            borrar_todo()
        elif opcion == "0":
            print("Saliendo del gestor...")
            break
        else:
            print("Opción no válida. Intenta de nuevo.")

if __name__ == "__main__":
    menu()