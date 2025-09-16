-- TABLAS DE ORIGEN --

-- Tabla de suscripciones con datos "sucios"
CREATE TABLE suscripciones_raw (
    id_suscripcion INT,
    id_cliente INT,
    id_producto INT,
    fecha_inicio_contrato STRING,
    fecha_fin_contrato_raw STRING, -- Campo que puede cambiar
    precio_suscripcion_raw STRING, -- Campo que puede cambiar
    id_ejecutivo INT
);

-- Tabla de clientes
CREATE TABLE clientes_raw (
    id_cliente INT,
    nombre_cliente STRING,
    industria STRING
);

-- Tabla de productos
CREATE TABLE productos_raw (
    id_producto INT,
    nombre_producto STRING,
    tipo_licencia STRING
);

-- TABLAS DE DESTINO (SCD TIPO 2) --

-- Tabla Maestra: Contiene el estado actual de cada suscripción
CREATE TABLE suscripciones_maestra (
    id_suscripcion INT,
    id_cliente INT,
    nombre_cliente STRING,
    nombre_producto STRING,
    fecha_inicio_contrato DATE,
    fecha_fin_contrato DATE,
    precio_suscripcion DECIMAL(10, 2),
    version_cambio INT
);

-- Tabla de Historial: Almacena las versiones antiguas
CREATE TABLE suscripciones_historial (
    id_historial BIGINT GENERATED ALWAYS AS IDENTITY,
    id_suscripcion INT,
    id_cliente INT,
    fecha_fin_contrato_anterior DATE,
    precio_suscripcion_anterior DECIMAL(10, 2),
    fecha_modificacion TIMESTAMP
);


-- Datos iniciales para la primera carga

-- 20 registros iniciales en la tabla de suscripciones
INSERT INTO suscripciones_raw VALUES
(101, 1, 501, '2022-01-01', '2023-01-01', '50.00 USD', 10),
(102, 2, 502, '2022-02-15', '2023-02-15', '100.00 USD', 10),
(103, 3, 503, '2022-03-20', '2023-03-20', '75.00 USD', 20),
(104, 4, 501, '2022-04-10', '2023-04-10', '50.00 USD', 10),
(105, 5, 502, '2022-05-01', '2023-05-01', '100.00 USD', 30),
(106, 6, 503, '2022-06-05', '2023-06-05', '75.00 USD', 20),
(107, 7, 501, '2022-07-20', '2023-07-20', '50.00 USD', 10),
(108, 8, 502, '2022-08-10', '2023-08-10', '100.00 USD', 30),
(109, 9, 503, '2022-09-01', '2023-09-01', '75.00 USD', 20),
(110, 10, 501, '2022-10-15', '2023-10-15', '50.00 USD', 10),
(111, 11, 502, '2022-11-05', '2023-11-05', '100.00 USD', 30),
(112, 12, 503, '2022-12-01', '2023-12-01', '75.00 USD', 20),
(113, 13, 501, '2023-01-25', '2024-01-25', '50.00 USD', 10),
(114, 14, 502, '2023-02-01', '2024-02-01', '100.00 USD', 30),
(115, 15, 503, '2023-03-10', '2024-03-10', '75.00 USD', 20),
(116, 16, 501, '2023-04-20', '2024-04-20', '50.00 USD', 10),
(117, 17, 502, '2023-05-15', '2024-05-15', '100.00 USD', 30),
(118, 18, 503, '2023-06-01', '2024-06-01', '75.00 USD', 20),
(119, 19, 501, '2023-07-10', '2024-07-10', '50.00 USD', 10),
(120, 20, 502, '2023-08-01', '2024-08-01', '100.00 USD', 30);


-- Datos de clientes
INSERT INTO clientes_raw VALUES
(1, 'TechCorp', 'Tecnología'), (2, 'Innovate Ltd', 'Servicios'), (3, 'Global Solutions', 'Consultoría'),
(4, 'DataStream', 'Tecnología'), (5, 'FutureWorks', 'Finanzas'), (6, 'EcoSystems', 'Medio Ambiente'),
(7, 'AlphaSoft', 'Tecnología'), (8, 'PrimeConnect', 'Finanzas'), (9, 'Synergy Corp', 'Consultoría'),
(10, 'SwiftTech', 'Tecnología'), (11, 'Innovate Now', 'Servicios'), (12, 'Peak Performance', 'Finanzas'),
(13, 'SecureCode', 'Tecnología'), (14, 'Nexus Solutions', 'Consultoría'), (15, 'GreenEnergy', 'Medio Ambiente'),
(16, 'QuantuMetric', 'Tecnología'), (17, 'Dynamic Flow', 'Finanzas'), (18, 'Apex Consulting', 'Consultoría'),
(19, 'CyberShield', 'Tecnología'), (20, 'FinServe', 'Finanzas');

-- Datos de productos
INSERT INTO productos_raw VALUES
(501, 'Software Suite Pro', 'Suscripción Anual'),
(502, 'Business Analytics Plus', 'Suscripción Anual'),
(503, 'Cloud Storage Enterprise', 'Suscripción Anual');



-- Primera carga de datos
MERGE INTO suscripciones_maestra AS target
USING (
    SELECT
        s.id_suscripcion,
        s.id_cliente,
        c.nombre_cliente,
        p.nombre_producto,
        TO_DATE(s.fecha_inicio_contrato, 'yyyy-MM-dd') AS fecha_inicio_contrato,
        TO_DATE(s.fecha_fin_contrato_raw, 'yyyy-MM-dd') AS fecha_fin_contrato,
        CAST(REGEXP_REPLACE(s.precio_suscripcion_raw, '[^0-9.]', '') AS DECIMAL(10, 2)) AS precio_suscripcion
    FROM suscripciones_raw AS s
    JOIN clientes_raw AS c ON s.id_cliente = c.id_cliente
    JOIN productos_raw AS p ON s.id_producto = p.id_producto
) AS source
ON target.id_suscripcion = source.id_suscripcion
WHEN NOT MATCHED THEN
    INSERT (id_suscripcion, id_cliente, nombre_cliente, nombre_producto, fecha_inicio_contrato, fecha_fin_contrato, precio_suscripcion, version_cambio)
    VALUES (
        source.id_suscripcion,
        source.id_cliente,
        source.nombre_cliente,
        source.nombre_producto,
        source.fecha_inicio_contrato,
        source.fecha_fin_contrato,
        source.precio_suscripcion,
        1
    );

-- Verificamos el resultado de la primera carga
SELECT * FROM suscripciones_maestra ORDER BY id_suscripcion;

-- Y el historial (debe estar vacío)
SELECT * FROM suscripciones_historial;


-- TRUNCATE TABLE suscripciones_raw; -- Limpiamos la tabla de origen
-- Inserta un nuevo lote de datos que incluye:
-- - Nuevos registros (id_suscripcion 121 y 122)
-- - Actualizaciones de precio (id_suscripcion 101 y 103)
-- - Actualización de fecha de contrato (id_suscripcion 102)

TRUNCATE TABLE suscripciones_raw;

INSERT INTO suscripciones_raw VALUES
(101, 1, 501, '2022-01-01', '2023-01-01', '55.00 USD', 10),    -- Precio actualizado
(102, 2, 502, '2022-02-15', '2024-02-15', '100.00 USD', 10), -- Fecha de fin de contrato actualizada
(103, 3, 503, '2022-03-20', '2023-03-20', '80.00 USD', 20),    -- Precio actualizado
(121, 21, 501, '2023-09-01', '2024-09-01', '60.00 USD', 10),    -- Nuevo registro
(122, 22, 502, '2023-10-15', '2024-10-15', '110.00 USD', 30);  -- Nuevo registro


-- Ejecutamos la fusión con la lógica SCD Tipo 2
-- 1. First, update the master table with MERGE (no INSERT in WHEN MATCHED)
MERGE INTO suscripciones_maestra AS target
USING (
    SELECT
        s.id_suscripcion,
        s.id_cliente,
        c.nombre_cliente,
        p.nombre_producto,
        TO_DATE(s.fecha_inicio_contrato, 'yyyy-MM-dd') AS fecha_inicio_contrato,
        TO_DATE(s.fecha_fin_contrato_raw, 'yyyy-MM-dd') AS fecha_fin_contrato,
        CAST(REGEXP_REPLACE(s.precio_suscripcion_raw, '[^0-9.]', '') AS DECIMAL(10, 2)) AS precio_suscripcion
    FROM suscripciones_raw AS s
    JOIN clientes_raw AS c ON s.id_cliente = c.id_cliente
    JOIN productos_raw AS p ON s.id_producto = p.id_producto
) AS source
ON target.id_suscripcion = source.id_suscripcion
WHEN MATCHED AND (
    target.precio_suscripcion != source.precio_suscripcion
    OR target.fecha_fin_contrato != source.fecha_fin_contrato
) THEN
    UPDATE SET
        target.fecha_fin_contrato = source.fecha_fin_contrato,
        target.precio_suscripcion = source.precio_suscripcion,
        target.version_cambio = target.version_cambio + 1
WHEN NOT MATCHED THEN
    INSERT (
        id_suscripcion,
        id_cliente,
        nombre_cliente,
        nombre_producto,
        fecha_inicio_contrato,
        fecha_fin_contrato,
        precio_suscripcion,
        version_cambio
    )
    VALUES (
        source.id_suscripcion,
        source.id_cliente,
        source.nombre_cliente,
        source.nombre_producto,
        source.fecha_inicio_contrato,
        source.fecha_fin_contrato,
        source.precio_suscripcion,
        1
    );

-- 2. Then, insert the previous values into the historial table
INSERT INTO suscripciones_historial (
    id_suscripcion,
    id_cliente,
    fecha_fin_contrato_anterior,
    precio_suscripcion_anterior,
    fecha_modificacion
)
SELECT
    t.id_suscripcion,
    t.id_cliente,
    t.fecha_fin_contrato,
    t.precio_suscripcion,
    CURRENT_TIMESTAMP()
FROM suscripciones_maestra t
JOIN (
    SELECT
        s.id_suscripcion,
        CAST(REGEXP_REPLACE(s.precio_suscripcion_raw, '[^0-9.]', '') AS DECIMAL(10, 2)) AS new_precio,
        TO_DATE(s.fecha_fin_contrato_raw, 'yyyy-MM-dd') AS new_fecha_fin
    FROM suscripciones_raw s
) s
ON t.id_suscripcion = s.id_suscripcion
WHERE
    t.precio_suscripcion != s.new_precio
    OR t.fecha_fin_contrato != s.new_fecha_fin;

-- Verificamos el estado final de las tablas
SELECT * FROM suscripciones_maestra ORDER BY id_suscripcion;
SELECT * FROM suscripciones_historial ORDER BY id_suscripcion;
