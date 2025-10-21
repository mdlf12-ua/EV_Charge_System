CREATE TABLE ChargingPoint (
    ID VARCHAR(20) PRIMARY KEY,
    Ubicacion VARCHAR(100),
    PRECIO DECIMAL(6,2),
    ESTADO ENUM('Activado','Parado','Suministrando') NOT NULL,
    CONDUCTOR_ID VARCHAR(20),
    CONSUMO_KW DECIMAL(6,2),   
    IMPORTE_EU DECIMAL(6,2)    
);