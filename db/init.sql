-- Tabla ÚNICA para toda la información del CP
CREATE TABLE IF NOT EXISTS ChargingPoint (
    -- Identificación
    ID VARCHAR(50) PRIMARY KEY,
    Ubicacion VARCHAR(100),
    
    -- Operación (datos dinámicos de Central)
    PRECIO DECIMAL(6,2) DEFAULT 0.30,
    ESTADO ENUM('REGISTRADO','ACTIVADO','PARADO','SUMINISTRANDO','DESCONECTADO','AVERIADO','AUTORIZADO') 
        NOT NULL DEFAULT 'REGISTRADO',
    CONDUCTOR_ID VARCHAR(20),
    CONSUMO_KW DECIMAL(6,2) DEFAULT 0.0,
    IMPORTE_EU DECIMAL(6,2) DEFAULT 0.0,
    ALERTA_METEO TINYINT(1) NOT NULL DEFAULT 0,
    
    -- Registro y Autenticación (datos de Registry)
    token VARCHAR(64),              -- Token de Registry para autenticación inicial
    encryption_key VARCHAR(64),     -- Clave de cifrado simétrico de Central
    registrado TINYINT(1) DEFAULT 0,    -- 0=no registrado, 1=registrado en Registry
    authenticated TINYINT(1) DEFAULT 0, -- 0=no autenticado, 1=autenticado en Central
    
    -- Auditoría
    fecha_registro TIMESTAMP NULL,
    fecha_auth TIMESTAMP NULL,
    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Tabla de alertas meteorológicas (se mantiene separada, es información externa)
CREATE TABLE IF NOT EXISTS WeatherAlert (
    location VARCHAR(100) PRIMARY KEY,
    alert_active TINYINT(1) NOT NULL DEFAULT 0,
    last_temp DECIMAL(6,2) NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Índices para consultas frecuentes
CREATE INDEX idx_estado ON ChargingPoint(ESTADO);
CREATE INDEX idx_ubicacion ON ChargingPoint(Ubicacion);
CREATE INDEX idx_authenticated ON ChargingPoint(authenticated);


