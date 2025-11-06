#!/usr/bin/env python3
import subprocess
import sys

DOCKER_COMPOSE_FILE = "docker-compose.yml"

def abrir_driver():
    mon = input("Número del driver que quieres abrir: ").strip()
    nombre_servicio = f"driver{mon}" if mon else "driver"
    subprocess.run(["docker", "compose", "run", nombre_servicio])

def abrir_todos():
   subprocess.run(["docker", "compose", "up", "-d"])

def borrar_driver():
    mon = input("Número del driver que quieres abrir: ").strip()
    nombre_servicio = f"driver{mon}" if mon else "driver"
    subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "stop", nombre_servicio])
    subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "rm", "-f", nombre_servicio])

def borrar_todo():
    subprocess.run(["docker", "compose", "down", "-v", "--rmi", "all"])

def menu():
    while True:
        print("\n=================================")
        print("  GESTOR DE CONTENEDORES DOCKER  ")
        print("=================================")
        print("1 - Abrir un DRIVER")
        print("2 - Borrar DRIVER")
        print("3 - Abrir TODOS")
        print("4 - Borrar TODOS")
        print("0 - Salir")
        print("=================================")

        opcion = input("Selecciona una opción: ").strip()

        if opcion == "1":
            abrir_driver()
        elif opcion == "2":
            borrar_driver()
        elif opcion == "3":
            abrir_todos()
        elif opcion == "4":
            borrar_todo()
        elif opcion == "0":
            print("Saliendo del gestor...")
            break
        else:
            print("Opción no válida. Intenta de nuevo.")

if __name__ == "__main__":
    menu()