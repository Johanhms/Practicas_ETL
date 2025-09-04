-- Tabla 1: Productos (Tabla de destino)
CREATE TABLE productos (
    id INT PRIMARY KEY,
    nombre VARCHAR(100),
    precio DECIMAL(10, 2),
    stock INT,
    ultima_actualizacion TIMESTAMP
);

-- Tabla 2: Actualizaciones_Fuente (Tabla de origen)
CREATE TABLE actualizaciones_fuente (
    id INT,
    nombre VARCHAR(100),
    precio DECIMAL(10, 2),
    stock INT
);

-- Tabla 3: Historial_Transacciones (Tabla de historia)
CREATE TABLE historial_transacciones2 (
    id SERIAL PRIMARY KEY,
    producto_id INT,
    campo_cambiado VARCHAR(50),
    valor_anterior VARCHAR(100),
    valor_nuevo VARCHAR(100),
    tipo_operacion VARCHAR(20),
    fecha_transaccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);