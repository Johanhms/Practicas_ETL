import pandas as pd
import psycopg2
from psycopg2 import sql

ruta_csv = r'c:\Users\johan\AdventureWorks-oltp-install-script'
nombre_tabla = 'department'
esquema_db = 'humanresources'
host = 'localhost'
dbname = 'Adventureworks'
user = 'postgres'
password = 'Mancity01.'
port = '5432'

def cargar_csv_a_postgres(ruta_csv, nombre_tabla, esquema_db, host, dbname, user, password, port):
    """
    Carga datos de un archivo CSV a una tabla en PostgreSQL.

    Args:
        ruta_csv (str): La ruta completa del archivo CSV a cargar.
        nombre_tabla (str): El nombre de la tabla de destino en la base de datos.
        esquema_db (str): El nombre del esquema de la base de datos (por ejemplo, 'humanresources').
        host (str): La dirección del host de la base de datos.
        dbname (str): El nombre de la base de datos.
        user (str): El nombre de usuario para la conexión.
        password (str): La contraseña para la conexión.
        port (str): El puerto de la base de datos.
    """
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

        # 2. Leer el archivo CSV con pandas
        # Usamos el delimitador de tabulación y un manejo de errores robusto para la codificación
        try:
            df = pd.read_csv(ruta_csv, delimiter='\t', encoding='latin-1')
        except UnicodeDecodeError:
            df = pd.read_csv(ruta_csv, delimiter='\t', encoding='cp1252')
        
        print(f"✅ Archivo CSV '{ruta_csv}' leído con éxito. Filas a insertar: {len(df)}")
        
        # 3. Preparar la consulta SQL de inserción
        # Definimos las columnas de la tabla de destino
        columnas_de_la_tabla = ["DepartmentID", "Name", "GroupName", "ModifiedDate"]
        
        # Usamos `psycopg2.sql` para construir de manera segura la consulta SQL
        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(esquema_db),
            sql.Identifier(nombre_tabla),
            sql.SQL(', ').join(sql.Identifier(col) for col in columnas_de_la_tabla),
            sql.SQL(', ').join(sql.Placeholder() for _ in columnas_de_la_tabla)
        )
        
        # 4. Insertar cada fila del DataFrame en la tabla
        for index, row in df.iterrows():
            try:
                # Nos aseguramos de que el orden de los datos del CSV coincida
                # con el orden de las columnas de la tabla.
                valores_a_insertar = [
                    row['DepartmentID'],
                    row['Name'],
                    row['GroupName'],
                    row['ModifiedDate']
                ]
                cur.execute(query, valores_a_insertar)
            except Exception as e:
                print(f"❌ Error al insertar la fila {index}: {e}")
                
        # 5. Confirmar los cambios
        conn.commit()
        print(f"🎉 ¡Datos de '{ruta_csv}' cargados exitosamente en la tabla '{esquema_db}.{nombre_tabla}'!")

    except psycopg2.Error as e:
        print(f"❌ Error de conexión a la base de datos: {e}")
    except FileNotFoundError:
        print(f"❌ Error: El archivo CSV no se encontró en la ruta especificada: {ruta_csv}")
    finally:
        # Cerrar la conexión y el cursor
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Conexión cerrada.")

if __name__ == '__main__':
    # ... (tu configuración de la conexión)
    DB_HOST = "localhost"
    DB_NAME = "AdventureWorks-oltp-install-script"
    DB_USER = "postgres"
    DB_PASSWORD = "your_password"
    DB_PORT = "5432"

    CSV_FILE_PATH = "Department.csv"
    TABLE_NAME = "department"
    SCHEMA_NAME = "humanresources"

    cargar_csv_a_postgres(CSV_FILE_PATH, TABLE_NAME, SCHEMA_NAME, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)