-- Datos iniciales en la tabla 'productos'
INSERT INTO productos (id, nombre, precio, stock) VALUES
(1, 'Laptop', 1200.00, 50),
(2, 'Mouse', 25.00, 200),
(3, 'Teclado', 75.00, 150);

-- Datos nuevos de la tabla 'actualizaciones_fuente'
-- Este es el "lote" de datos que vas a procesar con MERGE.
INSERT INTO actualizaciones_fuente (id, nombre, precio, stock) VALUES
(1, 'Laptop', 1150.00, 50), -- Actualización de precio
(2, 'Mouse', 25.00, 180),  -- Actualización de stock
(4, 'Monitor', 300.00, 75);  -- Nuevo producto

MERGE INTO productos AS destino
USING actualizaciones_fuente AS origen
ON destino.id = origen.id
WHEN MATCHED THEN
    UPDATE SET
        nombre = origen.nombre,
        precio = origen.precio,
        stock = origen.stock
WHEN NOT MATCHED THEN
    INSERT (id, nombre, precio, stock)
    VALUES (origen.id, origen.nombre, origen.precio, origen.stock);

select * from productos