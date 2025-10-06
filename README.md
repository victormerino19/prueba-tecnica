# Servicio de Ingesta de Datos (FastAPI + PostgreSQL)

Este proyecto implementa un servicio REST para ingesta, validación, respaldo y restauración de datos en una base de datos SQL (PostgreSQL). Soporta cargas por lotes, validación con modelos de datos, reglas de calidad, y generación/restauración de respaldos en formatos AVRO o PARQUET.

## Descripción
- Origen de datos: archivos CSV separados por comas y cargas REST en JSON.
- Base de datos destino: PostgreSQL.
- Funcionalidades: servicio REST con endpoints para ingesta, respaldo, restauración y borrado seguro.
- Lenguaje: Python (FastAPI, Pydantic, psycopg2, pyarrow/fastavro).

Seguridad y acceso
- Autenticación: API key en cabecera `X-API-Key` si `API_KEY` está definido en `.env`.
- Rate limiting: ventana configurable por `RATE_LIMIT_WINDOW_SECONDS` y máximo `RATE_LIMIT_MAX_REQUESTS` por IP.
- CORS: orígenes permitidos configurados con `ALLOWED_ORIGINS` en `.env` (coma separada o `*`).
- UI: el JS ahora se sirve desde `GET /static/ui.js`. En la página `/ui` puedes introducir la API key. Opcionalmente, si defines `EXPOSE_API_KEY_IN_UI=true`, el campo se prellenará con el valor de `API_KEY` del `.env`.

Variables en `.env`
- `API_KEY="tu_clave_segura"`
- `EXPOSE_API_KEY_IN_UI=false` (true para prellenar el campo en `/ui`)
- `ALLOWED_ORIGINS="http://127.0.0.1:8000,http://localhost:8000"` (o `*` para todos)
- `RATE_LIMIT_WINDOW_SECONDS=60`
- `RATE_LIMIT_MAX_REQUESTS=100`

Uso rápido con API key
- Inicia el servidor: `py -m uvicorn fast_api_con_rest:app --host 127.0.0.1 --port 8000`.
- Abre `http://127.0.0.1:8000/ui` y coloca tu API key en el campo superior. Si `EXPOSE_API_KEY_IN_UI=true`, verás el campo prellenado.
- Envía transacciones y genera respaldos; la cabecera `X-API-Key` se incluirá automáticamente.

Nota sobre UI estática
- El archivo `static/ui.js` encapsula la lógica de la UI. Si modificas el JS, no necesitas cambiar `fast_api_con_rest.py`.
 
 Advertencia
- No uses `EXPOSE_API_KEY_IN_UI=true` en producción: mostrará tu API key en el navegador. Úsalo solo para entornos locales/demos.

## Requisitos
- Python 3.11 o superior.
- PostgreSQL accesible con credenciales válidas.
- Paquetes Python: `fastapi`, `uvicorn`, `pydantic`, `psycopg2`, `python-dotenv`, `pyarrow`, `fastavro`.

## Configuración
1. Crear el archivo `.env` en la raíz con estas variables:
   - `DB_NAME=tu_base`
   - `DB_USER=tu_usuario`
   - `DB_PASSWORD=tu_password`
   - `DB_HOST=localhost`
   - `DB_PORT=5432`
2. Verifica que PostgreSQL esté corriendo y accesible.

## Inicio rápido
- Arranca el servidor en Windows:
  - `py -m uvicorn fast_api_con_rest:app --host 127.0.0.1 --port 8000`
- UI del servicio:
  - `http://127.0.0.1:8000/ui`
- Documentación interactiva (Swagger):
  - `http://127.0.0.1:8000/docs`

## Endpoints principales
- `POST /transacciones`: recibir registros por tabla (uno o varios grupos).
- `POST /respaldos`: generar respaldos AVRO/PARQUET por tabla.
- `GET /respaldos/existe`: listar respaldos disponibles.
- `DELETE /limpiar_tabla`: borrar una tabla si existen respaldos recientes.
- `POST /restaurar`: restaurar una tabla desde un respaldo.

## Lotes y validaciones
- Cada grupo en `/transacciones` acepta entre 1 y 1000 registros.
- Validación contra los modelos Pydantic:
  - `departamentos`: `id > 0`, `departamento` longitud 1–50.
  - `trabajos`: `id > 0`, `trabajo` longitud 1–200.
  - `empleados_contratados`: `id > 0`, `nombre` opcional (≤100), `fecha_hora` ISO-8601 opcional, `id_departamento` e `id_trabajo` opcionales (`NULL` permitido). Si se envían, deben ser `> 0` y existir como FK.
- UPSERT en lote con `page_size = 1090` para eficiencia.

## Respaldos y restauración
- Para respaldos: se exporta el contenido completo de cada tabla (`SELECT *`).
- Formatos:
  - PARQUET: recomendado, soporta `NULL` en FKs de empleados.
  - AVRO: válido para la mayoría de casos; si los datos de empleados incluyen `NULL` en FKs, prefiera PARQUET.
- Restauración:
  - Lee el archivo, valida contra modelos, aplica reglas de calidad y realiza UPSERT.
  - Respuesta indica `recibidos`, `validos`, `restaurados` y detalla errores de modelo/calidad.

## Importación desde CSV
- Estructura CSV separada por comas.
- CSV de ejemplo incluidos: `departments.csv`, `jobs.csv`, `hired_employees.csv`.
- El servicio REST espera JSON; los CSV históricos pueden integrarse vía scripts auxiliares (ver `modelos.py` para inserciones en lote mediante `COPY FROM`).

## Ejemplo de uso de /transacciones
Payload JSON para cargar departamentos, trabajos y empleados (con FKs nulas permitidas):

```json
{
  "departamentos": [
    { "id": 10, "departamento": "Ingeniería" },
    { "id": 20, "departamento": "Recursos Humanos" }
  ],
  "trabajos": [
    { "id": 100, "trabajo": "Desarrollador Senior" },
    { "id": 200, "trabajo": "Gerente de Contratación" }
  ],
  "empleados_contratados": [
    {
      "id": 1,
      "nombre": "Juan Perez",
      "fecha_hora": "2023-01-15T10:00:00",
      "id_departamento": 10,
      "id_trabajo": 100
    },
    {
      "id": 2,
      "nombre": "Maria Garcia",
      "fecha_hora": "2023-02-20T11:30:00",
      "id_departamento": 20,
      "id_trabajo": 200
    },
    {
      "id": 3,
      "nombre": "Carlos Lopez",
      "fecha_hora": "2023-03-01T09:00:00",
      "id_departamento": null,
      "id_trabajo": 100
    },
    {
      "id": 4,
      "nombre": "Ana Martinez",
      "fecha_hora": "2023-04-10T14:00:00",
      "id_departamento": 10,
      "id_trabajo": null
    },
    {
      "id": 5,
      "nombre": "Pedro Sanchez",
      "fecha_hora": "2023-05-05T16:00:00",
      "id_departamento": null,
      "id_trabajo": null
    }
  ]
}
```

- En Swagger (`/docs`): usa "Try it out" en `POST /transacciones` y pega el JSON.
- En la UI (`/ui`): hay acciones para generar respaldos, listar y restaurar.

## Ejemplo de generación de respaldos
- En la UI (`/ui`), sección "Generar respaldos": selecciona formato (parquet recomendado) y presiona el botón.
- Respuesta muestra cantidad de registros y ruta de cada archivo creado en `respaldos/`.

## Observaciones
- Para empleados con FKs nulas, usa respaldos PARQUET al restaurar.
- El `DELETE /limpiar_tabla` solo procede si se detectan respaldos disponibles para la tabla (por seguridad).
- El esquema de BD se asegura automáticamente al iniciar el servidor.

## Solución de problemas
- Errores tipo "input should be a valid integer" en restauración con AVRO: indica columnas FK con `null` y esquema AVRO sin tipo nulo. Usa PARQUET para estos casos.
- Verifica `.env` y conectividad a PostgreSQL si el servidor no inicia.