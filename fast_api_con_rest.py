import os
from typing import Any, Dict, List, Optional, Tuple

from datetime import datetime
from fastapi import FastAPI, Body, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from collections import defaultdict
import time
from pydantic import BaseModel, Field, constr, ValidationError
import psycopg2
import psycopg2.extras as pgextras
from dotenv import load_dotenv


# Cargar variables de entorno desde .env
load_dotenv()

# =============================
# Seguridad (API key, Rate limiting, CORS)
# =============================
API_KEY = os.getenv('API_KEY')
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '*')
EXPOSE_API_KEY_IN_UI = os.getenv('EXPOSE_API_KEY_IN_UI', 'false').strip().lower() in ('1','true','yes')
_RATE_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW_SECONDS', '60'))
_RATE_MAX = int(os.getenv('RATE_LIMIT_MAX_REQUESTS', '100'))
_rate_store: Dict[str, List[float]] = defaultdict(list)

def api_key_required(request: Request):
    """Valida API key en cabecera 'X-API-Key' si está configurada."""
    if API_KEY:
        header_key = request.headers.get('x-api-key') or request.headers.get('X-API-Key')
        if header_key != API_KEY:
            raise HTTPException(status_code=401, detail="API key inválida")
    return None

def rate_limiter(request: Request):
    """Limita solicitudes por IP en una ventana deslizante en memoria."""
    now = time.time()
    ip = request.client.host if request.client else 'unknown'
    arr = _rate_store[ip]
    cutoff = now - _RATE_WINDOW
    # Prune timestamps fuera de ventana
    i = 0
    while i < len(arr) and arr[i] < cutoff:
        i += 1
    if i > 0:
        del arr[:i]
    if len(arr) >= _RATE_MAX:
        raise HTTPException(status_code=429, detail="Demasiadas solicitudes, intente más tarde")
    arr.append(now)
    return None


# =============================
# Conexión a la base de datos
# =============================
def obtener_conexion_db():
    """Establece conexión con la base de datos PostgreSQL.

    Nota: Usa variables de entorno definidas en .env.
    """
    try:
        # Parámetros básicos
        params = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
        }

        # Soporte SSL opcional (útil para Azure PostgreSQL)
        sslmode = os.getenv('DB_SSLMODE')  # e.g., 'require', 'verify-ca', 'verify-full'
        sslrootcert = os.getenv('DB_SSLROOTCERT')  # ruta a CA si se usa verificación
        if sslmode:
            params['sslmode'] = sslmode
        if sslrootcert:
            # No validamos existencia aquí para permitir rutas en contenedores/montajes
            params['sslrootcert'] = sslrootcert

        conexion = psycopg2.connect(**params)
        return conexion
    except Exception as e:
        raise RuntimeError(f"Error al conectar a la base de datos: {e}")


# =============================
# Diccionario de datos (esquemas)
# =============================
class RegistroDepartamento(BaseModel):
    id: int = Field(gt=0, description="Identificador positivo del departamento")
    departamento: constr(min_length=1, max_length=50)


class RegistroTrabajo(BaseModel):
    id: int = Field(gt=0, description="Identificador positivo del trabajo")
    trabajo: constr(min_length=1, max_length=200)


class RegistroEmpleado(BaseModel):
    id: int = Field(gt=0, description="Identificador positivo del empleado")
    nombre: Optional[constr(max_length=100)] = None
    # Pydantic parsea ISO-8601 automáticamente (e.g. "2020-01-01T00:00:00")
    fecha_hora: Optional[datetime] = None
    # Permitir NULL en FKs; cuando estén presentes deben ser > 0
    id_departamento: Optional[int] = Field(default=None, gt=0)
    id_trabajo: Optional[int] = Field(default=None, gt=0)


TABLAS_VALIDAS = {
    "departamentos": RegistroDepartamento,
    "trabajos": RegistroTrabajo,
    "empleados_contratados": RegistroEmpleado,
}


# =============================
# Reglas de calidad específicas
# =============================
def validar_reglas_calidad(tabla: str, registros: List[BaseModel], conexion) -> Tuple[List[BaseModel], List[Dict[str, Any]]]:
    """Aplica reglas de calidad por tabla y retorna (registros_validos, errores).

    - departamentos: longitud de texto <= 50 ya validada por esquema.
    - trabajos: longitud de texto <= 200 ya validada por esquema.
    - empleados_contratados: validar existencia de FKs (id_departamento, id_trabajo).
    """
    errores: List[Dict[str, Any]] = []
    registros_validos: List[BaseModel] = []

    if tabla == "empleados_contratados":
        # Verificación de llaves foráneas en lote
        ids_dep = {r.id_departamento for r in registros if getattr(r, 'id_departamento', None) is not None}
        ids_job = {r.id_trabajo for r in registros if getattr(r, 'id_trabajo', None) is not None}

        try:
            with conexion.cursor() as cursor:
                # Validar departamentos existentes
                cursor.execute(
                    "SELECT id FROM departamentos WHERE id = ANY(%s)",
                    (list(ids_dep),),
                )
                dep_validos = {row[0] for row in cursor.fetchall()}

                # Validar trabajos existentes
                cursor.execute(
                    "SELECT id FROM trabajos WHERE id = ANY(%s)",
                    (list(ids_job),),
                )
                job_validos = {row[0] for row in cursor.fetchall()}

            # Clasificar cada registro según FKs válidas
            for idx, r in enumerate(registros):
                fk_ok = True
                # Solo validar si el valor está presente (no NULL)
                if r.id_departamento is not None and r.id_departamento not in dep_validos:
                    errores.append({
                        "indice": idx,
                        "tabla": tabla,
                        "detalle": f"id_departamento {r.id_departamento} no existe",
                    })
                    fk_ok = False
                if r.id_trabajo is not None and r.id_trabajo not in job_validos:
                    errores.append({
                        "indice": idx,
                        "tabla": tabla,
                        "detalle": f"id_trabajo {r.id_trabajo} no existe",
                    })
                    fk_ok = False
                if fk_ok:
                    registros_validos.append(r)
        except Exception as e:
            raise RuntimeError(f"Error validando reglas de calidad de empleados: {e}")
    else:
        # Para departamentos y trabajos, el esquema ya asegura tipos/longitudes
        registros_validos = registros

    return registros_validos, errores


# =============================
# Operaciones de inserción (UPSERT)
# =============================
def upsert_departamentos(conexion, registros: List[RegistroDepartamento]) -> int:
    """Inserta/actualiza departamentos en lote con ON CONFLICT (UPSERT)."""
    if not registros:
        return 0
    valores = [(r.id, r.departamento) for r in registros]
    sql = (
        "INSERT INTO departamentos (id, departamento) VALUES %s "
        "ON CONFLICT (id) DO UPDATE SET departamento = EXCLUDED.departamento"
    )
    try:
        with conexion.cursor() as cursor:
            pgextras.execute_values(cursor, sql, valores, page_size=1090)
        conexion.commit()
        return len(registros)
    except Exception as e:
        conexion.rollback()
        raise RuntimeError(f"Error al upsert departamentos: {e}")


def upsert_trabajos(conexion, registros: List[RegistroTrabajo]) -> int:
    """Inserta/actualiza trabajos en lote con ON CONFLICT (UPSERT)."""
    if not registros:
        return 0
    valores = [(r.id, r.trabajo) for r in registros]
    sql = (
        "INSERT INTO trabajos (id, trabajo) VALUES %s "
        "ON CONFLICT (id) DO UPDATE SET trabajo = EXCLUDED.trabajo"
    )
    try:
        with conexion.cursor() as cursor:
            pgextras.execute_values(cursor, sql, valores, page_size=1090)
        conexion.commit()
        return len(registros)
    except Exception as e:
        conexion.rollback()
        raise RuntimeError(f"Error al upsert trabajos: {e}")


def upsert_empleados(conexion, registros: List[RegistroEmpleado]) -> int:
    """Inserta/actualiza empleados en lote con ON CONFLICT (UPSERT)."""
    if not registros:
        return 0
    valores = [
        (
            r.id,
            r.nombre,
            r.fecha_hora,
            r.id_departamento,
            r.id_trabajo,
        )
        for r in registros
    ]
    sql = (
        "INSERT INTO empleados_contratados (id, nombre, fecha_hora, id_departamento, id_trabajo) VALUES %s "
        "ON CONFLICT (id) DO UPDATE SET "
        "nombre = EXCLUDED.nombre, "
        "fecha_hora = EXCLUDED.fecha_hora, "
        "id_departamento = EXCLUDED.id_departamento, "
        "id_trabajo = EXCLUDED.id_trabajo"
    )
    try:
        with conexion.cursor() as cursor:
            pgextras.execute_values(cursor, sql, valores, page_size=1090)
        conexion.commit()
        return len(registros)
    except Exception as e:
        conexion.rollback()
        raise RuntimeError(f"Error al upsert empleados: {e}")


# =============================
# Servicio REST
# =============================
app = FastAPI(title="Servicio de Ingesta de Datos", version="1.0.0")

# CORS configurado desde ALLOWED_ORIGINS en .env (coma separada o '*')
_origins = ["*"] if ALLOWED_ORIGINS.strip() == '*' else [o.strip() for o in ALLOWED_ORIGINS.split(',') if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Servir archivos estáticos (UI JS)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Exponer esquema de seguridad ApiKey en OpenAPI para facilitar pruebas desde Swagger
from fastapi.openapi.utils import get_openapi

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Servicio de Ingesta de Datos",
        version="1.0.0",
        description="API de ingesta, respaldos y restauración de datos.",
        routes=app.routes,
    )
    # Definir esquema de seguridad por API Key en cabecera
    components = openapi_schema.setdefault("components", {})
    security_schemes = components.setdefault("securitySchemes", {})
    security_schemes["ApiKeyAuth"] = {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
    }
    # Aplicar seguridad global a todas las operaciones (solo para UI de Swagger)
    for path_item in openapi_schema.get("paths", {}).values():
        for operation in path_item.values():
            if isinstance(operation, dict):
                sec = operation.get("security", [])
                sec.append({"ApiKeyAuth": []})
                operation["security"] = sec
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi


# =============================
# Middleware de seguridad sencillo (opcional por API_KEY)
# =============================
class SimpleSecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        path = request.url.path
        # Permitir libre acceso a UI estática y rutas de recursos del IDE
        if (
            path == '/'
            or path.startswith('/ui')
            or path.startswith('/static')
            or path.startswith('/healthz')
            or path.startswith('/@')  # p. ej. /@vite/client en vistas del IDE
            or path.startswith('/docs')  # Swagger UI y sus assets
            or path == '/openapi.json'   # Esquema OpenAPI consumido por Swagger
            or path.startswith('/redoc') # Redoc UI
        ):
            return await call_next(request)

        # Validación simple basada en variable de entorno API_KEY (si definida)
        try:
            api_key_required(request)
        except HTTPException as e:
            return PlainTextResponse(e.detail, status_code=e.status_code)

        # Rate limiting
        try:
            rate_limiter(request)
        except HTTPException as e:
            return PlainTextResponse(e.detail, status_code=e.status_code)

        return await call_next(request)

app.add_middleware(SimpleSecurityMiddleware)


def asegurar_esquema(conexion) -> None:
    """Crea tablas si no existen, sin borrar datos existentes."""
    try:
        with conexion.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS departamentos (
                    id INTEGER PRIMARY KEY,
                    departamento VARCHAR(50) NOT NULL
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS trabajos (
                    id INTEGER PRIMARY KEY,
                    trabajo VARCHAR(200) NOT NULL
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS empleados_contratados (
                    id INTEGER PRIMARY KEY,
                    nombre VARCHAR(100),
                    fecha_hora TIMESTAMP,
                    id_departamento INTEGER REFERENCES departamentos(id),
                    id_trabajo INTEGER REFERENCES trabajos(id)
                );
                """
            )
            # El esquema previo no incluye tabla de usuarios/api keys
        conexion.commit()
    except Exception as e:
        conexion.rollback()
        raise RuntimeError(f"Error al asegurar esquema: {e}")


@app.on_event("startup")
def _on_startup():
    """Evento de arranque: asegurar que el esquema existe."""
    conexion = obtener_conexion_db()
    try:
        asegurar_esquema(conexion)
    finally:
        conexion.close()

# =============================
# Healthcheck simple
# =============================
@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    """Verifica que la app responde y que la DB está accesible."""
    try:
        con = obtener_conexion_db()
        try:
            with con.cursor() as cur:
                cur.execute("SELECT 1")
                _ = cur.fetchone()
        finally:
            con.close()
        return "ok"
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"unhealthy: {e}")

# =============================
# Métricas trimestrales (Desafío #2)
# =============================
@app.get("/metricas/contrataciones_por_trimestre")
def metricas_contrataciones_por_trimestre(anio: int, incluir_nulos: bool = False):
    """Cantidad de empleados contratados en 'anio' por departamento y cargo, dividido por trimestre.

    - Ordena alfabéticamente por departamento y luego por cargo.
    - Si `incluir_nulos=true`, agrupa NULL como 'Sin asignar'.
    - Requiere API key si está configurada (middleware global).
    """
    conexion = obtener_conexion_db()
    try:
        with conexion.cursor(cursor_factory=pgextras.RealDictCursor) as cur:
            if incluir_nulos:
                sql = (
                    "SELECT "
                    "  COALESCE(d.departamento, 'Sin asignar') AS department, "
                    "  COALESCE(j.trabajo, 'Sin asignar') AS job, "
                    "  SUM(CASE WHEN EXTRACT(MONTH FROM e.fecha_hora) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS q1, "
                    "  SUM(CASE WHEN EXTRACT(MONTH FROM e.fecha_hora) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS q2, "
                    "  SUM(CASE WHEN EXTRACT(MONTH FROM e.fecha_hora) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS q3, "
                    "  SUM(CASE WHEN EXTRACT(MONTH FROM e.fecha_hora) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS q4 "
                    "FROM empleados_contratados e "
                    "LEFT JOIN departamentos d ON e.id_departamento = d.id "
                    "LEFT JOIN trabajos j ON e.id_trabajo = j.id "
                    "WHERE e.fecha_hora IS NOT NULL AND EXTRACT(YEAR FROM e.fecha_hora) = %s "
                    "GROUP BY department, job "
                    "ORDER BY department ASC, job ASC"
                )
                cur.execute(sql, (anio,))
            else:
                sql = (
                    "SELECT "
                    "  d.departamento AS department, "
                    "  j.trabajo AS job, "
                    "  SUM(CASE WHEN EXTRACT(MONTH FROM e.fecha_hora) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS q1, "
                    "  SUM(CASE WHEN EXTRACT(MONTH FROM e.fecha_hora) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS q2, "
                    "  SUM(CASE WHEN EXTRACT(MONTH FROM e.fecha_hora) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS q3, "
                    "  SUM(CASE WHEN EXTRACT(MONTH FROM e.fecha_hora) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS q4 "
                    "FROM empleados_contratados e "
                    "JOIN departamentos d ON e.id_departamento = d.id "
                    "JOIN trabajos j ON e.id_trabajo = j.id "
                    "WHERE e.fecha_hora IS NOT NULL AND EXTRACT(YEAR FROM e.fecha_hora) = %s "
                    "GROUP BY d.departamento, j.trabajo "
                    "ORDER BY d.departamento ASC, j.trabajo ASC"
                )
                cur.execute(sql, (anio,))
            filas = cur.fetchall()
            return [
                {
                    "department": f.get("department"),
                    "job": f.get("job"),
                    "q1": int(f.get("q1", 0) or 0),
                    "q2": int(f.get("q2", 0) or 0),
                    "q3": int(f.get("q3", 0) or 0),
                    "q4": int(f.get("q4", 0) or 0),
                }
                for f in filas
            ]
    finally:
        conexion.close()

# =============================
# Restauración desde AVRO/PARQUET y verificación de respaldos
# =============================
def leer_avro_archivo(ruta_archivo: str) -> List[Dict[str, Any]]:
    """Lee un archivo AVRO y devuelve lista de dicts."""
    try:
        from fastavro import reader
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dependencia fastavro no disponible: {e}")
    if not os.path.exists(ruta_archivo):
        raise HTTPException(status_code=400, detail=f"Archivo no encontrado: {ruta_archivo}")
    try:
        with open(ruta_archivo, 'rb') as f:
            return list(reader(f))
    except Exception as e:
        raise RuntimeError(f"Error leyendo AVRO: {e}")


def leer_parquet_archivo(ruta_archivo: str) -> List[Dict[str, Any]]:
    """Lee un archivo PARQUET y devuelve lista de dicts."""
    try:
        import pyarrow.parquet as pq
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dependencia pyarrow no disponible: {e}")
    if not os.path.exists(ruta_archivo):
        raise HTTPException(status_code=400, detail=f"Archivo no encontrado: {ruta_archivo}")
    try:
        table = pq.read_table(ruta_archivo)
        return table.to_pylist()
    except Exception as e:
        raise RuntimeError(f"Error leyendo PARQUET: {e}")


# Registro por email eliminado para restaurar comportamiento previo sin flujo de correo.


def _listar_respaldos_por_tabla(tabla: str, directorio: str = "respaldos", solo_hoy: bool = False) -> Dict[str, Any]:
    """Lista respaldos .avro/.parquet para una tabla. Puede filtrar solo los de hoy."""
    if tabla not in TABLAS_VALIDAS:
        raise HTTPException(status_code=400, detail=f"Tabla no soportada: {tabla}")
    if not isinstance(directorio, str) or not directorio:
        raise HTTPException(status_code=400, detail="'directorio' debe ser una cadena no vacía")
    if not os.path.exists(directorio):
        return {"existen": False, "total_archivos": 0, "archivos": [], "solo_hoy": solo_hoy}

    import re
    patron = re.compile(rf"^{tabla}_(\d{{8}})_\d{{6}}\.(avro|parquet)$")
    hoy = datetime.now().strftime('%Y%m%d')
    archivos = []
    for nombre in os.listdir(directorio):
        m = patron.match(nombre)
        if not m:
            continue
        fecha = m.group(1)
        if solo_hoy and fecha != hoy:
            continue
        archivos.append({
            "archivo": nombre,
            "ruta": os.path.join(directorio, nombre),
            "fecha": fecha,
            "formato": nombre.split('.')[-1],
        })

    return {
        "existen": len(archivos) > 0,
        "total_archivos": len(archivos),
        "archivos": archivos,
        "solo_hoy": solo_hoy,
        "directorio": directorio,
        "tabla": tabla,
    }


@app.get("/respaldos/existe")
def respaldos_existe(tabla: str, directorio: str = "respaldos", solo_hoy: bool = True):
    """Verifica si existen respaldos (AVRO/PARQUET) para la tabla. Por defecto, solo los de hoy."""
    return _listar_respaldos_por_tabla(tabla, directorio, solo_hoy)


@app.delete("/limpiar_tabla")
def limpiar_tabla(payload: Dict[str, Any] = Body(..., description="Borra todos los registros de una tabla si hay respaldo")):
    """
    Payload esperado:
    {
      "tabla": "departamentos" | "trabajos" | "empleados_contratados",
      "directorio": "respaldos" (opcional, por defecto),
      "solo_hoy": true (opcional, por defecto true)
    }
    """
    tabla = payload.get('tabla')
    directorio = payload.get('directorio') or 'respaldos'
    solo_hoy = payload.get('solo_hoy', True)

    if tabla not in TABLAS_VALIDAS:
        raise HTTPException(status_code=400, detail="'tabla' debe ser una tabla válida")

    info = _listar_respaldos_por_tabla(tabla, directorio, solo_hoy=bool(solo_hoy))
    if not info["existen"]:
        raise HTTPException(status_code=400, detail=f"No hay respaldos {'de hoy ' if solo_hoy else ''}.avro o .parquet para '{tabla}' en '{directorio}'")

    # Borrado seguro de datos de la tabla
    conexion = obtener_conexion_db()
    try:
        borrados = 0
        with conexion.cursor() as cursor:
            cursor.execute(f"DELETE FROM {tabla}")
            borrados = cursor.rowcount if cursor.rowcount is not None else 0
        conexion.commit()
        return {
            "tabla": tabla,
            "borrados": borrados,
            "respaldos": info,
        }
    except Exception as e:
        conexion.rollback()
        raise HTTPException(status_code=500, detail=f"Error al borrar datos de '{tabla}': {e}")
    finally:
        conexion.close()


@app.post("/restaurar")
def restaurar(payload: Dict[str, Any] = Body(..., description="Restaura una tabla desde archivo AVRO/PARQUET")):
    """
    Payload esperado:
    {
      "formato": "avro" | "parquet",
      "tabla": "departamentos" | "trabajos" | "empleados_contratados",
      "archivo": "ruta/al/archivo.avro|parquet"
    }
    """
    formato = payload.get('formato')
    tabla = payload.get('tabla')
    archivo = payload.get('archivo')

    if formato not in {"avro", "parquet"}:
        raise HTTPException(status_code=400, detail="'formato' debe ser 'avro' o 'parquet'")
    if tabla not in TABLAS_VALIDAS:
        raise HTTPException(status_code=400, detail="'tabla' debe ser una tabla válida")
    if not isinstance(archivo, str) or not archivo:
        raise HTTPException(status_code=400, detail="'archivo' debe ser una cadena no vacía")

    # Medir duración total
    _ts_ini = datetime.now()
    # Leer registros del archivo
    if formato == 'avro':
        registros = leer_avro_archivo(archivo)
    else:
        registros = leer_parquet_archivo(archivo)

    # Convertir tipos con modelos locales y aplicar reglas de calidad
    conexion = obtener_conexion_db()
    try:
        # Paso 1: Parsear a modelos Pydantic locales según TABLAS_VALIDAS
        registros_modelo, errores_modelo = _parsear_registros_para_tabla(tabla, registros)

        # Paso 2: Aplicar reglas de calidad (FKs para empleados)
        registros_validos, errores_calidad = validar_reglas_calidad(tabla, registros_modelo, conexion)

        # Paso 3: UPSERT por tabla
        cantidad = 0
        if tabla == "departamentos":
            cantidad = upsert_departamentos(conexion, registros_validos)  # type: ignore[arg-type]
        elif tabla == "trabajos":
            cantidad = upsert_trabajos(conexion, registros_validos)  # type: ignore[arg-type]
        elif tabla == "empleados_contratados":
            cantidad = upsert_empleados(conexion, registros_validos)  # type: ignore[arg-type]

        _dur_ms = int((datetime.now() - _ts_ini).total_seconds() * 1000)
        return {
            "tabla": tabla,
            "restaurados": cantidad,
            "recibidos": len(registros),
            "validos": len(registros_validos),
            "errores_modelo": errores_modelo,
            "errores_calidad": errores_calidad,
            "duracion_ms": _dur_ms,
        }
    finally:
        conexion.close()

def _parsear_registros_para_tabla(tabla: str, datos: List[Dict[str, Any]]) -> Tuple[List[BaseModel], List[Dict[str, Any]]]:
    """Convierte dicts a modelos Pydantic de la tabla dada. Retorna (validos, errores)."""
    if tabla not in TABLAS_VALIDAS:
        raise HTTPException(status_code=400, detail=f"Tabla no soportada: {tabla}")
    modelo = TABLAS_VALIDAS[tabla]
    registros_validos: List[BaseModel] = []
    errores: List[Dict[str, Any]] = []
    for idx, item in enumerate(datos):
        try:
            registros_validos.append(modelo(**item))
        except ValidationError as ve:
            errores.append({
                "indice": idx,
                "tabla": tabla,
                "detalle": ve.errors(),
            })
    return registros_validos, errores


@app.post("/transacciones")
def recibir_transacciones(payload: Dict[str, Any] = Body(..., description="Carga de registros por tabla")):
    """Endpoint único para recibir registros de cualquier tabla.

    Formatos soportados de payload:

    1) Un solo grupo:
       {
         "tabla": "departamentos" | "trabajos" | "empleados_contratados",
         "registros": [ { ... }, { ... } ]
       }

    2) Múltiples grupos por clave:
       {
         "departamentos": [ { ... } ],
         "trabajos": [ { ... } ],
         "empleados_contratados": [ { ... } ]
       }

    Reglas generales:
    - Soporta lotes entre 1 y 1000 registros por grupo.
    - Valida contra el diccionario de datos antes de aceptar.
    - Aplica reglas de calidad específicas por tabla.
    """
    grupos: Dict[str, List[Dict[str, Any]]] = {}

    # Normalizar payload a diccionario de grupos { tabla: [registros] }
    if "tabla" in payload and "registros" in payload:
        tabla = payload["tabla"]
        registros = payload["registros"]
        if not isinstance(registros, list):
            raise HTTPException(status_code=400, detail="'registros' debe ser una lista")
        grupos[tabla] = registros
    else:
        # Asumir claves por nombre de tabla
        for tabla in TABLAS_VALIDAS.keys():
            if tabla in payload:
                registros = payload[tabla]
                if not isinstance(registros, list):
                    raise HTTPException(status_code=400, detail=f"'{tabla}' debe ser una lista")
                grupos[tabla] = registros

    if not grupos:
        raise HTTPException(status_code=400, detail="Payload vacío o sin claves de tablas válidas")

    # Validación de tamaños de lote
    for tabla, registros in grupos.items():
        n = len(registros)
        if n < 1 or n > 1000:
            raise HTTPException(status_code=400, detail=f"El grupo '{tabla}' debe contener entre 1 y 1000 registros")

    conexion = obtener_conexion_db()

    resumen: Dict[str, Any] = {"procesados": {}, "errores": []}

    try:
        for tabla, datos in grupos.items():
            # Paso 1: Validación contra el diccionario de datos
            registros_modelo, errores_modelo = _parsear_registros_para_tabla(tabla, datos)
            resumen["errores"].extend(errores_modelo)

            # Paso 2: Reglas de calidad por tabla
            registros_validos, errores_calidad = validar_reglas_calidad(tabla, registros_modelo, conexion)
            resumen["errores"].extend(errores_calidad)

            # Paso 3: Inserción/actualización (UPSERT)
            cantidad = 0
            if tabla == "departamentos":
                cantidad = upsert_departamentos(conexion, registros_validos)  # type: ignore[arg-type]
            elif tabla == "trabajos":
                cantidad = upsert_trabajos(conexion, registros_validos)  # type: ignore[arg-type]
            elif tabla == "empleados_contratados":
                cantidad = upsert_empleados(conexion, registros_validos)  # type: ignore[arg-type]

            resumen["procesados"][tabla] = {
                "recibidos": len(datos),
                "validos": len(registros_validos),
                "upsert": cantidad,
            }

        return resumen
    finally:
        conexion.close()


# =============================
# Respaldos en AVRO o PARQUET
# =============================
def obtener_datos_tabla(conexion, tabla: str) -> List[Dict[str, Any]]:
    """Obtiene todos los registros de una tabla como lista de dicts."""
    if tabla not in TABLAS_VALIDAS:
        raise HTTPException(status_code=400, detail=f"Tabla no soportada: {tabla}")
    try:
        with conexion.cursor(cursor_factory=pgextras.RealDictCursor) as cursor:
            cursor.execute(f"SELECT * FROM {tabla}")
            filas = cursor.fetchall()
            # Convertir datetime a ISO-8601 string para exportar estable
            registros: List[Dict[str, Any]] = []
            for row in filas:
                d = dict(row)
                if 'fecha_hora' in d and d['fecha_hora'] is not None:
                    # Asegurar formato string para compatibilidad de exportación
                    d['fecha_hora'] = d['fecha_hora'].isoformat()
                registros.append(d)
            return registros
    except Exception as e:
        raise RuntimeError(f"Error obteniendo datos de {tabla}: {e}")


def _avro_schema_para_tabla(tabla: str) -> Dict[str, Any]:
    """Devuelve un esquema AVRO simple para la tabla."""
    if tabla == 'departamentos':
        return {
            'name': 'departamentos',
            'type': 'record',
            'fields': [
                {'name': 'id', 'type': 'int'},
                {'name': 'departamento', 'type': 'string'},
            ],
        }
    if tabla == 'trabajos':
        return {
            'name': 'trabajos',
            'type': 'record',
            'fields': [
                {'name': 'id', 'type': 'int'},
                {'name': 'trabajo', 'type': 'string'},
            ],
        }
    if tabla == 'empleados_contratados':
        return {
            'name': 'empleados_contratados',
            'type': 'record',
            'fields': [
                {'name': 'id', 'type': 'int'},
                {'name': 'nombre', 'type': ['null', 'string']},
                {'name': 'fecha_hora', 'type': ['null', 'string']},
                {'name': 'id_departamento', 'type': 'int'},
                {'name': 'id_trabajo', 'type': 'int'},
            ],
        }
    raise HTTPException(status_code=400, detail=f"Tabla no soportada: {tabla}")


def exportar_avro_por_tabla(registros: List[Dict[str, Any]], tabla: str, ruta_archivo: str) -> int:
    """Exporta registros a AVRO en ruta_archivo. Devuelve cantidad de registros."""
    try:
        from fastavro import writer  # Lazy import para evitar fallos en startup
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dependencia fastavro no disponible: {e}")

    schema = _avro_schema_para_tabla(tabla)
    # Asegurar directorio
    os.makedirs(os.path.dirname(ruta_archivo), exist_ok=True)
    try:
        with open(ruta_archivo, 'wb') as f:
            writer(f, schema, registros)
        return len(registros)
    except Exception as e:
        raise RuntimeError(f"Error exportando AVRO para {tabla}: {e}")


def exportar_parquet_por_tabla(registros: List[Dict[str, Any]], tabla: str, ruta_archivo: str) -> int:
    """Exporta registros a PARQUET en ruta_archivo. Devuelve cantidad de registros."""
    try:
        import pyarrow as pa  # Lazy import
        import pyarrow.parquet as pq
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dependencia pyarrow no disponible: {e}")

    # Definir esquema simple (fecha_hora como string para compatibilidad)
    if tabla == 'departamentos':
        schema = pa.schema([
            ('id', pa.int32()),
            ('departamento', pa.string()),
        ])
    elif tabla == 'trabajos':
        schema = pa.schema([
            ('id', pa.int32()),
            ('trabajo', pa.string()),
        ])
    elif tabla == 'empleados_contratados':
        schema = pa.schema([
            ('id', pa.int32()),
            ('nombre', pa.string()),
            ('fecha_hora', pa.string()),
            ('id_departamento', pa.int32()),
            ('id_trabajo', pa.int32()),
        ])
    else:
        raise HTTPException(status_code=400, detail=f"Tabla no soportada: {tabla}")

    # Asegurar directorio
    os.makedirs(os.path.dirname(ruta_archivo), exist_ok=True)
    try:
        table = pa.Table.from_pylist(registros, schema=schema)
        pq.write_table(table, ruta_archivo)
        return len(registros)
    except Exception as e:
        raise RuntimeError(f"Error exportando PARQUET para {tabla}: {e}")


@app.post("/respaldos")
def generar_respaldos(payload: Dict[str, Any] = Body(..., description="Genera respaldos AVRO/PARQUET por tabla")):
    """Genera copias de seguridad por tabla en formato AVRO o PARQUET.

    Payload esperado:
    {
      "formato": "avro" | "parquet",
      "tablas": ["departamentos", "trabajos", "empleados_contratados"],  # opcional, por defecto todas
      "directorio": "respaldos"  # opcional
    }
    """
    formato = payload.get('formato')
    if formato not in {"avro", "parquet"}:
        raise HTTPException(status_code=400, detail="'formato' debe ser 'avro' o 'parquet'")

    tablas = payload.get('tablas')
    if tablas is None:
        tablas = list(TABLAS_VALIDAS.keys())
    if not isinstance(tablas, list) or not tablas:
        raise HTTPException(status_code=400, detail="'tablas' debe ser una lista no vacía")

    directorio = payload.get('directorio') or 'respaldos'
    if not isinstance(directorio, str) or not directorio:
        raise HTTPException(status_code=400, detail="'directorio' debe ser una cadena no vacía")

    conexion = obtener_conexion_db()
    resultado: List[Dict[str, Any]] = []

    try:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        _t0 = datetime.now()
        for tabla in tablas:
            if tabla not in TABLAS_VALIDAS:
                raise HTTPException(status_code=400, detail=f"Tabla no soportada: {tabla}")

            registros = obtener_datos_tabla(conexion, tabla)
            nombre_archivo = f"{tabla}_{ts}.{ 'avro' if formato == 'avro' else 'parquet' }"
            ruta_archivo = os.path.join(directorio, nombre_archivo)

            if formato == 'avro':
                cantidad = exportar_avro_por_tabla(registros, tabla, ruta_archivo)
            else:
                cantidad = exportar_parquet_por_tabla(registros, tabla, ruta_archivo)

            resultado.append({
                'tabla': tabla,
                'formato': formato,
                'ruta': ruta_archivo,
                'registros': cantidad,
            })

        _dur_ms_total = int((datetime.now() - _t0).total_seconds() * 1000)
        return {
            'respaldos': resultado,
            'directorio': directorio,
            'formato': formato,
            'duracion_ms_total': _dur_ms_total,
        }
    finally:
        conexion.close()

# =============================
# UI simple para pruebas
# =============================
def _html_ui() -> str:
    default_api_key = API_KEY if EXPOSE_API_KEY_IN_UI and API_KEY else ''
    html = """
<!DOCTYPE html>
<html lang=\"es\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>UI de Pruebas</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    h1 { margin-bottom: 0; }
    h2 { margin-top: 24px; }
    .card { border: 1px solid #ddd; padding: 16px; border-radius: 8px; margin-bottom: 20px; }
    textarea { width: 100%; height: 180px; font-family: monospace; }
    label { display: block; margin: 8px 0 4px; }
    .row { display: flex; gap: 16px; }
    .row > div { flex: 1; }
    .btn { padding: 8px 12px; border: 1px solid #333; border-radius: 6px; background: #f5f5f5; cursor: pointer; }
    .btn:hover { background: #e9e9e9; }
    .result { white-space: pre-wrap; background: #fafafa; border: 1px solid #eee; padding: 12px; border-radius: 6px; }
    .small { color: #666; font-size: 12px; }
  </style>
  <script src="/static/ui.js"></script>
  </head>
  <body>
    <h1>UI de Pruebas</h1>
    <p class=\"small\">Probar endpoints de ingesta (/transacciones) y respaldos (/respaldos).</p>

    <div class=\"card\">
      <h2>Autenticación</h2>
      <p>Introduce tu API key si tu servidor la requiere.</p>
      <label>API Key</label>
      <input id=\"api-key-input\" type=\"text\" placeholder=\"{DEFAULT_API_KEY}\" value=\"{DEFAULT_API_KEY}\" />
      <div style=\"margin-top:8px;\" class=\"small\">La API key se guarda localmente en tu navegador.</div>
    </div>

    <div class=\"card\">
      <h2>Transacciones</h2>
      <p>Enviar JSON para uno o varios grupos de tablas.</p>
      <label>Payload JSON</label>
      <textarea id=\"tx-body\"></textarea>
      <div style=\"margin-top:8px;\">
        <button class=\"btn\" onclick=\"loadSampleTransacciones()\">Cargar ejemplo</button>
        <button class=\"btn\" onclick=\"postTransacciones()\">Enviar a /transacciones</button>
      </div>
      <h3>Resultado</h3>
      <div id=\"tx-result\" class=\"result\"></div>
    </div>

    <div class=\"card\">
      <h2>Respaldos</h2>
      <div class=\"row\">
        <div>
          <label>Formato</label>
          <select id=\"bk-formato\">
            <option value=\"parquet\">parquet</option>
            <option value=\"avro\">avro</option>
          </select>
        </div>
        <div>
          <label>Directorio destino</label>
          <input id=\"bk-dir\" type=\"text\" placeholder=\"respaldos\" />
        </div>
      </div>
      <label>Tablas</label>
      <div>
        <label><input id=\"bk-dep\" type=\"checkbox\" value=\"departamentos\" /> Departamentos</label>
        <label><input id=\"bk-job\" type=\"checkbox\" value=\"trabajos\" /> Trabajos</label>
        <label><input id=\"bk-emp\" type=\"checkbox\" value=\"empleados_contratados\" /> Empleados contratados</label>
      </div>
      <div style=\"margin-top:8px;\">
        <button class=\"btn\" onclick=\"loadSampleRespaldos()\">Cargar ejemplo</button>
        <button class=\"btn\" onclick=\"postRespaldos()\">Generar /respaldos</button>
      </div>
      <h3 style=\"margin-top:20px;\">Eliminar X tabla (solo si hay respaldos de hoy)</h3>
      <div class=\"row\">
        <div>
          <label>Tabla a borrar</label>
          <select id=\"del-tabla\">
            <option value=\"departamentos\">departamentos</option>
            <option value=\"trabajos\">trabajos</option>
            <option value=\"empleados_contratados\">empleados_contratados</option>
          </select>
        </div>
        <div style=\"align-self:flex-end;\">
          <button class=\"btn\" onclick=\"borrarTablaSiHayRespaldo()\">Eliminar X tabla</button>
        </div>
      </div>
      <h3>Resultado</h3>
      <div id=\"bk-result\" class=\"result\"></div>
      <div id=\"del-result\" class=\"result\" style=\"margin-top:8px;\"></div>

      <h3 style=\"margin-top:20px;\">Restaurar desde respaldo</h3>
      <div class=\"row\">
        <div>
          <label>Tabla a restaurar</label>
          <select id=\"rs-tabla\">
            <option value=\"departamentos\">departamentos</option>
            <option value=\"trabajos\">trabajos</option>
            <option value=\"empleados_contratados\">empleados_contratados</option>
          </select>
        </div>
        <div>
          <label>Directorio de respaldos</label>
          <input id=\"rs-dir\" type=\"text\" placeholder=\"respaldos\" />
        </div>
      </div>
      <div style=\"margin-top:8px;\">
        <button class=\"btn\" onclick=\"listarRespaldos()\">Listar respaldos</button>
      </div>
      <div style=\"margin-top:8px;\">
        <label>Seleccione respaldo</label>
        <select id=\"rs-archivos\" style=\"min-width: 320px;\">
          <option value=\"\">-- Sin datos --</option>
        </select>
      </div>
      <div style=\"margin-top:8px;\">
        <button class=\"btn\" onclick=\"restaurarDesdeSeleccion()\">Restaurar</button>
      </div>
      <h3>Resultado</h3>
      <div id=\"rs-result\" class=\"result\"></div>
    </div>

  </body>
</html>
        """
    return html.replace("{DEFAULT_API_KEY}", default_api_key)


@app.get("/ui", response_class=HTMLResponse)
def ui_pruebas():
    return _html_ui()


# Redirigir raíz a la UI para evitar 404 en vistas del IDE
@app.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse(url="/ui")


# JavaScript externo para la UI
def _ui_js() -> str:
    return (
        """
// UI JS sin dependencias externas
function postTransacciones() {
  var ta = document.getElementById('tx-body');
  var apiKeyEl = document.getElementById('api-key');
  var apiKey = apiKeyEl ? apiKeyEl.value : '';
  var payload;
  try { payload = JSON.parse(ta.value); }
  catch (e) { document.getElementById('tx-result').textContent = 'JSON invalido: ' + e.message; return; }
  fetch('/transacciones', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
    body: JSON.stringify(payload)
  })
  .then(function(resp) {
    return resp.text().then(function(text) {
      document.getElementById('tx-result').textContent = 'HTTP ' + resp.status + '\\n\\n' + text;
    });
  })
  .catch(function(e) { document.getElementById('tx-result').textContent = 'Error: ' + e.message; });
}

function postRespaldos() {
  var apiKeyEl = document.getElementById('api-key');
  var apiKey = apiKeyEl ? apiKeyEl.value : '';
  var formato = document.getElementById('bk-formato').value;
  var dir = document.getElementById('bk-dir').value || 'respaldos';
  var tablas = [];
  var ids = ['bk-dep','bk-job','bk-emp'];
  for (var i = 0; i < ids.length; i++) {
    var el = document.getElementById(ids[i]);
    if (el && el.checked) { tablas.push(el.value); }
  }
  var payload = { formato: formato, directorio: dir, tablas: tablas };
  fetch('/respaldos', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
    body: JSON.stringify(payload)
  })
  .then(function(resp) {
    return resp.text().then(function(text) {
      var out = document.getElementById('bk-result');
      try {
        var json = JSON.parse(text);
        var dur = json.duracion_ms_total || json.duracion_ms || 0;
        var totalRegs = 0;
        if (Array.isArray(json.respaldos)) {
          for (var i = 0; i < json.respaldos.length; i++) {
            totalRegs += json.respaldos[i].registros || 0;
          }
        }
        out.textContent = 'Listo. Duró ' + dur + ' ms. Registros: ' + totalRegs;
      } catch (e) {
        out.textContent = 'HTTP ' + resp.status + ' - ' + text;
      }
    });
  })
  .catch(function(e) { document.getElementById('bk-result').textContent = 'Error: ' + e.message; });
}

function borrarTablaSiHayRespaldo() {
  var apiKeyEl = document.getElementById('api-key');
  var apiKey = apiKeyEl ? apiKeyEl.value : '';
  var tabla = document.getElementById('del-tabla').value;
  var dir = document.getElementById('bk-dir').value || 'respaldos';
  var out = document.getElementById('del-result');
  if (!tabla) { out.textContent = 'Error: seleccione una tabla a borrar'; return; }
  var url = '/respaldos/existe?tabla=' + encodeURIComponent(tabla) + '&directorio=' + encodeURIComponent(dir) + '&solo_hoy=true';
  fetch(url, { headers: { 'X-API-Key': apiKey } })
    .then(function(resp){ return resp.json().then(function(json){ return { status: resp.status, json: json }; }); })
    .then(function(res){
      if (res.status !== 200) { out.textContent = 'HTTP ' + res.status + ' - ' + JSON.stringify(res.json); return; }
      if (!res.json.existen) { out.textContent = 'No hay respaldos de hoy para ' + tabla + ' en ' + dir; return; }
      var confirmMsg = 'Se encontraron ' + res.json.total_archivos + ' respaldos de HOY para ' + tabla + '.\\n\\nDeseas borrar todos los registros de la tabla?';
      if (!window.confirm(confirmMsg)) { out.textContent = 'Accion cancelada por el usuario.'; return; }
      return fetch('/limpiar_tabla', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
        body: JSON.stringify({ tabla: tabla, directorio: dir, solo_hoy: true })
      })
      .then(function(resp){ return resp.text().then(function(text){ out.textContent = 'HTTP ' + resp.status + ' - ' + text; }); });
    })
    .catch(function(e){ out.textContent = 'Error: ' + e.message; });
}

function listarRespaldos() {
  var apiKeyEl = document.getElementById('api-key');
  var apiKey = apiKeyEl ? apiKeyEl.value : '';
  var tabla = document.getElementById('rs-tabla').value;
  var dir = document.getElementById('rs-dir').value || 'respaldos';
  var select = document.getElementById('rs-archivos');
  var out = document.getElementById('rs-result');
  if (!tabla) { out.textContent = 'Error: seleccione una tabla'; return; }
  var url = '/respaldos/existe?tabla=' + encodeURIComponent(tabla) + '&directorio=' + encodeURIComponent(dir) + '&solo_hoy=false';
  fetch(url, { headers: { 'X-API-Key': apiKey } })
    .then(function(resp){ return resp.json().then(function(json){ return { status: resp.status, json: json }; }); })
    .then(function(res){
      if (res.status !== 200) { out.textContent = 'HTTP ' + res.status + ' - ' + JSON.stringify(res.json); return; }
      var archivos = res.json.archivos || [];
      while (select.firstChild) { select.removeChild(select.firstChild); }
      if (!archivos.length) {
        var opt = document.createElement('option'); opt.value = ''; opt.textContent = '-- No hay respaldos --'; select.appendChild(opt);
        out.textContent = 'No se encontraron respaldos en ' + dir + ' para ' + tabla; return;
      }
      archivos.sort(function(a,b){ return a.archivo < b.archivo ? 1 : -1; });
      archivos.forEach(function(item){
        var opt = document.createElement('option');
        opt.value = item.ruta;
        opt.textContent = item.archivo + ' (' + item.formato + ', ' + item.fecha + ')';
        select.appendChild(opt);
      });
      out.textContent = 'Respaldos listados: ' + archivos.length;
    })
    .catch(function(e){ out.textContent = 'Error: ' + e.message; });
}

function restaurarDesdeSeleccion() {
  var apiKeyEl = document.getElementById('api-key');
  var apiKey = apiKeyEl ? apiKeyEl.value : '';
  var tabla = document.getElementById('rs-tabla').value;
  var select = document.getElementById('rs-archivos');
  var ruta = select.value;
  var out = document.getElementById('rs-result');
  if (!ruta) { out.textContent = 'Error: seleccione un respaldo'; return; }
  var formato = 'parquet';
  if (ruta.toLowerCase().endsWith('.avro')) { formato = 'avro'; }
  var payload = { formato: formato, tabla: tabla, archivo: ruta };
  fetch('/restaurar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
    body: JSON.stringify(payload)
  })
  .then(function(resp){
    return resp.text().then(function(text){
      try {
        var json = JSON.parse(text);
        var dur = json.duracion_ms || 0;
        var cant = json.restaurados || json.upsert || 0;
        out.textContent = 'Listo. Restaurados: ' + cant + '. Duró ' + dur + ' ms.';
      } catch (e) {
        out.textContent = 'HTTP ' + resp.status + ' - ' + text;
      }
    });
  })
  .catch(function(e){ out.textContent = 'Error: ' + e.message; });
}

function loadSamples() {
  var sampleMulti = {
    departamentos: [{ id: 16, departamento: 'Calidad' }],
    trabajos: [{ id: 999, trabajo: 'Developer X' }],
    empleados_contratados: [{ id: 501, nombre: 'Ana', fecha_hora: '2020-01-01T00:00:00', id_departamento: 16, id_trabajo: 999 }]
  };
  document.getElementById('tx-body').value = JSON.stringify(sampleMulti, null, 2);
  var dep = document.getElementById('bk-dep');
  var job = document.getElementById('bk-job');
  var emp = document.getElementById('bk-emp');
  if (dep) { dep.checked = true; }
  if (job) { job.checked = true; }
  if (emp) { emp.checked = true; }
}

// Cargar solo ejemplo de Transacciones
function loadSampleTransacciones() {
  var sampleMulti = {
    departamentos: [{ id: 16, departamento: 'Calidad' }],
    trabajos: [{ id: 999, trabajo: 'Developer X' }],
    empleados_contratados: [{ id: 501, nombre: 'Ana', fecha_hora: '2020-01-01T00:00:00', id_departamento: 16, id_trabajo: 999 }]
  };
  document.getElementById('tx-body').value = JSON.stringify(sampleMulti, null, 2);
}

// Cargar solo ejemplo de Respaldos
function loadSampleRespaldos() {
  var formatoEl = document.getElementById('bk-formato');
  var dirEl = document.getElementById('bk-dir');
  if (formatoEl) formatoEl.value = 'parquet';
  if (dirEl) dirEl.value = 'respaldos';
  var dep = document.getElementById('bk-dep');
  var job = document.getElementById('bk-job');
  var emp = document.getElementById('bk-emp');
  if (dep) dep.checked = true;
  if (job) job.checked = true;
  if (emp) emp.checked = true;
}

document.addEventListener('DOMContentLoaded', function() {
  loadSamples();
});
        """
    )
