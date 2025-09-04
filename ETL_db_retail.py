import psycopg2
from psycopg2 import sql

esquema_origen = 'public'
esquema_destino = 'public'
tablas_a_mover = ['categories', 'customers']
host = 'localhost'
dbname = 'retail_db'
user = 'postgres'
password = 'Mancity01.'
port = '5432'

def mover_tablas_entre_esquemas(esquema_origen, esquema_destino, tablas_a_mover, host, dbname, user, password, port):
    """
    Mueve una lista de tablas de un esquema a otro en PostgreSQL.

    Args:
        esquema_origen (str): El nombre del esquema de origen (por ejemplo, 'retail_db').
        esquema_destino (str): El nombre del esquema de destino (por ejemplo, 'test_db').
        tablas_a_mover (list): Una lista de nombres de tablas a mover.
        host (str): La dirección del host de la base de datos.
        dbname (str): El nombre de la base de datos.
        user (str): El nombre de usuario para la conexión.
        password (str): La contraseña para la conexión.
        port (str): El puerto de la base de datos.
    """
    conn = None
    cur = None
    try:
        # 1. Conectar a la base de datos PostgreSQL
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        cur = conn.cursor()
        print("✅ Conexión a la base de datos exitosa.")

        # 2. Mover cada tabla de la lista
        for tabla in tablas_a_mover:
            try:
                # 3. Preparar la consulta SQL
                # La sintaxis es ALTER TABLE <esquema_origen>.<nombre_tabla> SET SCHEMA <esquema_destino>;
                query = sql.SQL("ALTER TABLE {}.{} SET SCHEMA {};").format(
                    sql.Identifier(esquema_origen),
                    sql.Identifier(tabla),
                    sql.Identifier(esquema_destino)
                )

                # Ejecutar la consulta
                cur.execute(query)
                conn.commit()
                print(f"🎉 Tabla '{tabla}' movida exitosamente a '{esquema_destino}'.")
            except Exception as e:
                print(f"❌ Error al mover la tabla '{tabla}': {e}")
                conn.rollback() # Revertir la transacción si hay un error

        print("🎉 Proceso completado.")

    except psycopg2.Error as e:
        print(f"❌ Error de conexión a la base de datos: {e}")
    finally:
        # Cerrar la conexión
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Conexión cerrada.")

if __name__ == '__main__':
    # --- Configuración de la conexión ---
    DB_HOST = 'localhost' 
    DB_NAME = 'retail_db'  # Cambia esto al nombre de tu BD
    DB_USER = 'postgres'
    DB_PASSWORD = 'Mancity01.' # ¡Cambia esto por tu contraseña!
    DB_PORT = "5432"

    # --- Configuración del proceso de movimiento ---
    ESQUEMA_ORIGEN = "public"
    ESQUEMA_DESTINO = "public"

    # Lista de tablas a mover.
    # Asegúrate de que los nombres coincidan exactamente con las tablas del esquema retail_db.
    TABLAS = ["categories", "customers", "departments", "order_items", "orders", "products"]

    # Llama a la función para mover las tablas
    mover_tablas_entre_esquemas(ESQUEMA_ORIGEN, ESQUEMA_DESTINO, TABLAS, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)