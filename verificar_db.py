import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def obtener_conexion_db():
    try:
        conexion = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        print("Conexión a la base de datos exitosa.")
        return conexion
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def contar_registros_en_tablas():
    conexion = None
    try:
        conexion = obtener_conexion_db()
        if conexion:
            with conexion.cursor() as cursor:
                tablas = ['departamentos', 'trabajos', 'empleados_contratados']
                for tabla in tablas:
                    cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                    count = cursor.fetchone()[0]
                    print(f"Tabla '{tabla}': {count} registros.")
    except Exception as e:
        print(f"Error al contar registros: {e}")
    finally:
        if conexion:
            conexion.close()

if __name__ == "__main__":
    contar_registros_en_tablas()