# EVCharging Network — Sistema Distribuido de Gestión de Recarga de Vehículos Eléctricos

> **Práctica de Sistemas Distribuidos · Universidad de Alicante · Curso 25/26**  
> Implementación de una red de puntos de recarga de vehículos eléctricos gestionada en tiempo real mediante arquitectura distribuida, comunicación por sockets, streaming de eventos con Apache Kafka y servicios REST.



---

## Componentes del Sistema

### `EV_Central` — Central de Control
Núcleo del sistema distribuido. Gestiona y monitoriza todos los puntos de recarga en tiempo real.

- Escucha en un puerto TCP configurable (sockets)
- Mantiene el estado de todos los CPs en base de datos
- Autoriza o deniega peticiones de suministro
- Consume y produce mensajes en **Apache Kafka**
- Muestra un panel de monitorización en tiempo real
- Implementa **registro de auditoría** de todos los eventos
- Gestiona cifrado simétrico por CP para comunicaciones seguras


---

### `EV_CP_E` — Charging Point Engine
Motor del punto de recarga. Gestiona el ciclo de vida del suministro de energía.

- Se conecta a **Apache Kafka** para recibir órdenes de la central
- Reporta telemetría en tiempo real (consumo en kW, importe en €) cada segundo
- Soporta modos: `ACTIVADO`, `PARADO`, `SUMINISTRANDO`, `AVERIADO`, `DESCONECTADO`
- Implementa **cifrado simétrico** en todos los mensajes enviados a la central

**Ejecución:**
```bash
python EV_CP_E.py <kafka_ip:puerto> <ev_cp_m_ip:puerto>
```

---

### `EV_CP_M` — Charging Point Monitor
Módulo de vigilancia del hardware y software del punto de recarga.

- Verifica el estado de salud del CP cada segundo vía socket
- Notifica averías a `EV_Central` en tiempo real
- Gestiona el **registro seguro** del CP en `EV_Registry` mediante HTTPS/TLS
- Recibe y almacena las claves de cifrado simétricas proporcionadas por la central



### `EV_Driver` — Aplicación del Conductor
Interfaz de usuario para solicitar recargas en los puntos de la red.

- Solicita suministros de forma manual o desde fichero de secuencias
- Recibe notificaciones en tiempo real del estado de su suministro vía **Kafka**
- Muestra todos los CPs disponibles en la red
- Espera 4 segundos entre solicitudes consecutivas del fichero



### `API_Central` — API REST de la Central
Exposición REST del estado del sistema para consumo externo.

- Endpoints GET/PUT para consultar CPs, Drivers y transacciones en curso
- Recibe alertas meteorológicas de `EV_W`
- Sirve datos al módulo **Front** (web pública)

---

### `EV_Registry` — Registro de Charging Points
Módulo de alta y baja de puntos de recarga en el sistema.

- Expone una **API REST** (GET, PUT, DELETE) para gestión de CPs
- Canal de comunicación **seguro (HTTPS/SSL/RSA)** con `EV_CP_M`
- Devuelve credenciales de autenticación al CP tras el registro
- Comparte base de datos con `EV_Central`

---

### `EV_W` — Weather Control Office
Módulo de control climático que condiciona la operación de los CPs.

- Consulta la temperatura de las localizaciones cada **4 segundos** vía **OpenWeatherMap API**
- Si temperatura < 0°C → notifica alerta a `EV_Central` → el CP pasa a `FUERA DE SERVICIO`
- Cuando la temperatura recupera → notifica la cancelación de la alerta

---

### `Front` — Panel Web de Monitorización
Interfaz web pública accesible desde cualquier navegador.

- Consume `API_Central` para mostrar el estado en tiempo real
- Muestra: estado de CPs, Drivers activos, temperatura por localización, alertas y errores

---

## Tecnologías Utilizadas

| Tecnología | Rol en el sistema |
|---|---|
| **Python** | Lenguaje principal de implementación de todos los módulos |
| **Apache Kafka** | Broker de mensajería para streaming de eventos en tiempo real entre CPs, Central y Drivers |
| **Sockets TCP** | Comunicación de estado y autenticación entre Monitor y Central |
| **REST API (HTTP/HTTPS)** | Registro de CPs, exposición del estado del sistema, integración con OpenWeatherMap |
| **SSL/TLS + RSA** | Canal seguro entre `EV_Registry` y `EV_CP_M` |
| **Cifrado simétrico (AES)** | Cifrado de mensajes entre CPs y Central en Kafka |
| **SQLite / MySQL / MongoDB** | Persistencia de datos de CPs, conductores y transacciones |
| **OpenWeatherMap API** | Datos meteorológicos en tiempo real |
| **Docker** | Contenedorización y despliegue distribuido|

---

## Apache Kafka — Mensajería Distribuida

Apache Kafka es el **núcleo de comunicación asíncrona** del sistema. Actúa como broker de eventos desacoplando productores y consumidores, lo que permite que el sistema sea **resiliente, escalable y tolerante a fallos**.


---

## Seguridad

El sistema implementa múltiples capas de seguridad:

1. **Cifrado de canal**: HTTPS/TLS entre `EV_Registry` ↔ `EV_CP_M`
2. **Autenticación segura**: Credenciales protegidas, sin transmisión en claro
3. **Cifrado simétrico por CP**: Clave única por CP para mensajes en Kafka entre CP y Central
4. **Revocación de claves**: `EV_Central` puede revocar claves de un CP, forzando re-autenticación
5. **Auditoría de eventos**: Registro estructurado de todos los eventos del sistema:

```json
{
  "timestamp": "2025-10-15T10:45:32Z",
  "source_ip": "192.168.1.45",
  "actor": "CP_ALC1",
  "action": "AUTHENTICATION_SUCCESS",
  "params": "CP registrado correctamente. Clave asignada."
}
```

---

## Base de Datos 

Accesible tanto por `EV_Central` como por `EV_Registry`. Almacena:

- Datos de los CPs: ID, ubicación, estado, precio €/kWh, credenciales
- Datos de conductores
- Historial de transacciones y suministros
- Claves de cifrado por CP
- Log de auditoría




**Asignatura:** Sistemas Distribuidos  
**Universidad:** Universidad de Alicante  
**Curso académico:** 2025/2026

---

## 📄 Licencia

Proyecto académico desarrollado para la asignatura de Sistemas Distribuidos. Uso educativo.
