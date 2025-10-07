# Imagen base ligera con Python
FROM python:3.11-slim

# Evitar interacción y optimizar pip
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Crear directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema requeridas por psycopg2 y pyarrow
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar el proyecto
COPY . ./

# Exponer puerto
EXPOSE 8000

# Comando por defecto: ejecuta Uvicorn apuntando al app FastAPI
CMD ["uvicorn", "fast_api_con_rest:app", "--host", "0.0.0.0", "--port", "8000"]