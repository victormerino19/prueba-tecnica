import time
import psycopg2
import csv
from datetime import datetime
from typing import List, Dict, Any
import os
import io
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def obtener_conexion_db():
    """Establece conexión con la base de datos PostgreSQL."""
    try:
        conexion = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        print(f"Conectado a la base de datos: {os.getenv('DB_NAME')}")
        return conexion
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        raise

def crear_tablas(conexion):
    """Crea las tablas necesarias en la base de datos."""
    try:
        with conexion.cursor() as cursor:
            # Eliminar tablas si existen
            cursor.execute("""
                DROP TABLE IF EXISTS empleados_contratados CASCADE;
                DROP TABLE IF EXISTS departamentos CASCADE;
                DROP TABLE IF EXISTS trabajos CASCADE;
            """)
            
            # Crear tabla departamentos
            cursor.execute("""
                CREATE TABLE departamentos (
                    id INTEGER PRIMARY KEY,
                    departamento VARCHAR(50) NOT NULL
                )
            """)
            
            # Crear tabla trabajos
            cursor.execute("""
                CREATE TABLE trabajos (
                    id INTEGER PRIMARY KEY,
                    trabajo VARCHAR(200) NOT NULL
                )
            """)
            
            # Crear tabla empleados_contratados
            cursor.execute("""
                CREATE TABLE empleados_contratados (
                    id INTEGER PRIMARY KEY,
                    nombre VARCHAR(100),
                    fecha_hora TIMESTAMP,
                    id_departamento INTEGER REFERENCES departamentos(id),
                    id_trabajo INTEGER REFERENCES trabajos(id)
                )
            """)
            
            conexion.commit()
            print("Tablas creadas correctamente")
    except Exception as e:
        conexion.rollback()
        print(f"Error al crear las tablas: {e}")
        raise

def contar_registros_tabla(conexion, nombre_tabla):
    """Cuenta el número de registros en una tabla."""
    try:
        with conexion.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {nombre_tabla}")
            return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error al contar registros de {nombre_tabla}: {e}")
        return 0

def procesar_csv_por_lotes(ruta_archivo: str, tamano_lote: int = 1000):
    """Lee un archivo CSV en lotes y los devuelve como un generador."""
    lote_actual = []
    total_registros = 0
    
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as archivo:
            lector_csv = csv.reader(archivo)
            # Saltar la cabecera si existe
            if ruta_archivo.endswith('hired_employees.csv'):
                next(lector_csv)
            
            for fila in lector_csv:
                # Para hired_employees.csv, asegurarse de que al menos el ID no esté vacío
                if ruta_archivo.endswith('hired_employees.csv'):
                    if fila and fila[0].strip():  # Verificar que al menos el ID existe
                        # Rellenar campos vacíos con None
                        fila_procesada = [campo.strip() if campo.strip() else None for campo in fila]
                        lote_actual.append(fila_procesada)
                        total_registros += 1
                else:
                    # Para otros archivos, verificar que todos los campos estén completos
                    if all(campo.strip() for campo in fila):
                        lote_actual.append(fila)
                        total_registros += 1
                
                if len(lote_actual) >= tamano_lote:
                    yield lote_actual
                    lote_actual = []
            
            # Devolver el último lote si existe
            if lote_actual:
                yield lote_actual
            
            print(f"Total de registros leídos de {ruta_archivo}: {total_registros}")
    except Exception as e:
        print(f"Error al procesar el archivo {ruta_archivo}: {e}")
        raise

def insertar_lote_departamentos(conexion, lote: List[List[str]]):
    """Inserta un lote de departamentos en la base de datos usando COPY FROM."""
    try:
        with conexion.cursor() as cursor:
            # Crear un objeto StringIO para simular un archivo en memoria
            output = io.StringIO()
            for fila in lote:
                # Asegurarse de que los datos estén en el formato correcto para COPY FROM
                # y manejar posibles valores None o vacíos
                line = f"{fila[0]}\t{fila[1]}\n"
                output.write(line)
            output.seek(0) # Volver al inicio del "archivo"
            
            cursor.copy_from(output, 'departamentos', columns=('id', 'departamento'))
            conexion.commit()
    except Exception as e:
        conexion.rollback()
        print(f"Error al insertar departamentos con COPY FROM: {e}")
        raise

def insertar_lote_trabajos(conexion, lote: List[List[str]]):
    """Inserta un lote de trabajos en la base de datos usando COPY FROM."""
    try:
        with conexion.cursor() as cursor:
            output = io.StringIO()
            for fila in lote:
                line = f"{fila[0]}\t{fila[1]}\n"
                output.write(line)
            output.seek(0)
            
            cursor.copy_from(output, 'trabajos', columns=('id', 'trabajo'))
            conexion.commit()
    except Exception as e:
        conexion.rollback()
        print(f"Error al insertar trabajos con COPY FROM: {e}")
        raise

def insertar_lote_empleados(conexion, lote: List[List[str]]):
    """Inserta un lote de empleados en la base de datos usando COPY FROM."""
    try:
        with conexion.cursor() as cursor:
            output = io.StringIO()
            lote_procesado = []
            for fila in lote:
                try:
                    if fila[0] is not None:
                        fecha_hora = None
                        if fila[2]:
                            try:
                                fecha_str = fila[2].replace('T', ' ')
                                fecha_hora = datetime.strptime(fecha_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                print(f"Error al procesar fecha para el empleado ID {fila[0]}")

                        # Formatear los datos para COPY FROM. Los valores None deben ser cadenas vacías para COPY FROM.
                        # Los enteros deben ser convertidos a cadena.
                        id_empleado = str(int(fila[0])) if fila[0] is not None else "\\N"
                        nombre_empleado = fila[1] if fila[1] is not None else "\\N"
                        fecha_hora_empleado = fecha_hora.strftime('%Y-%m-%d %H:%M:%S') if fecha_hora else "\\N"
                        id_departamento_empleado = str(int(fila[3])) if fila[3] is not None else "\\N"
                        id_trabajo_empleado = str(int(fila[4])) if fila[4] is not None else "\\N"

                        line = f"{id_empleado}\t{nombre_empleado}\t{fecha_hora_empleado}\t{id_departamento_empleado}\t{id_trabajo_empleado}\n"
                        output.write(line)
                        lote_procesado.append(fila) # Mantener un registro para el conteo
                except (ValueError, IndexError) as e:
                    print(f"Error procesando fila {fila}: {e}")
                    continue
            
            output.seek(0)
            if lote_procesado: # Solo intentar copiar si hay datos válidos
                cursor.copy_from(output, 'empleados_contratados', columns=('id', 'nombre', 'fecha_hora', 'id_departamento', 'id_trabajo'))
                conexion.commit()
                print(f"Insertados {len(lote_procesado)} empleados con COPY FROM")
    except Exception as e:
        conexion.rollback()
        print(f"Error al insertar empleados con COPY FROM: {e}")
        raise

def importar_todos_los_datos():
    """Función principal para importar todos los datos."""
    start_time = time.time() # Iniciar el temporizador
    try:
        conexion = obtener_conexion_db()
        
        # Crear tablas
        crear_tablas(conexion)
        
        # Importar departamentos
        print("\nImportando departamentos...")
        for i, lote in enumerate(procesar_csv_por_lotes('departments.csv'), 1):
            insertar_lote_departamentos(conexion, lote)
            print(f"Lote {i} de departamentos procesado")
        total_departamentos = contar_registros_tabla(conexion, 'departamentos')
        print(f"Total de departamentos importados: {total_departamentos}")
        
        # Importar trabajos
        print("\nImportando trabajos...")
        for i, lote in enumerate(procesar_csv_por_lotes('jobs.csv'), 1):
            insertar_lote_trabajos(conexion, lote)
            print(f"Lote {i} de trabajos procesado")
        total_trabajos = contar_registros_tabla(conexion, 'trabajos')
        print(f"Total de trabajos importados: {total_trabajos}")
        
        # Importar empleados
        print("\nImportando empleados...")
        for i, lote in enumerate(procesar_csv_por_lotes('hired_employees.csv'), 1):
            insertar_lote_empleados(conexion, lote)
            print(f"Lote {i} de empleados procesado")
        total_empleados = contar_registros_tabla(conexion, 'empleados_contratados')
        print(f"Total de empleados importados: {total_empleados}")
        
        print("\nResumen de la importación:")
        print(f"- Departamentos: {total_departamentos}")
        print(f"- Trabajos: {total_trabajos}")
        print(f"- Empleados: {total_empleados}")
        
        # Verificar algunos empleados importados
        with conexion.cursor() as cursor:
            cursor.execute("SELECT * FROM empleados_contratados LIMIT 5")
            print("\nPrimeros 5 empleados importados:")
            for empleado in cursor.fetchall():
                print(empleado)
        
        conexion.close()
        end_time = time.time() # Detener el temporizador
        duration = end_time - start_time
        print(f"\nProceso de importación completado con éxito en {duration:.2f} segundos.")
        
        # Escribir la duración en un archivo temporal
        with open("import_duration.txt", "w") as f:
            f.write(str(duration))
        
    except Exception as e:
        print(f"Error durante la importación: {e}")
        if 'conexion' in locals():
            conexion.close()
        raise

if __name__ == "__main__":
    importar_todos_los_datos()



