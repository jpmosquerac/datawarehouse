# Almacén de Datos — Guía de operación

Todos los comandos se ejecutan desde esta carpeta (`datawarehouse/`).

---

## 1. Levantar la base de datos

```bash
docker compose up -d postgres
```

Al crearse por primera vez el contenedor inicializa automáticamente:
- `metadata_repository` — repositorio de metadatos (capas 1–5)
- `datawarehouse` — almacén de datos con el schema `dw` (9 tablas)

Verificar que levantó correctamente:

```bash
docker compose ps
# Estado esperado: modelado_postgres   Up (healthy)
```

---

## 2. Cargar datos con los ETLs

Los tres ETLs deben correrse **en este orden**:

### ETL 1 — Descubrimiento de metadatos (Entrega 1)

Parsea los dumps fuente y puebla las capas 1–3 del repositorio de metadatos (metadata técnica, negocio y linaje).

```bash
docker compose run --rm etl_metadata
```

Salida esperada:
```
✓ customers (13 columnas)  ...  ✓ cs_products (5 columnas)
✓ Dominio: Ventas (6 entidades)  ...
✓ 85 relaciones registradas
✅ ETL completado exitosamente
```

### ETL 2 — Carga del almacén de datos (Entrega 2)

Parsea los dumps, carga las 7 dimensiones y las 2 tablas de hechos. Los registros con caracteres no-ASCII se quarentenan en `stg/registros_cuarentenados.parquet`.

```bash
docker compose run --rm etl_dw
```

Salida esperada:
```
customers=122, employees=23, offices=7
orders=326, orderdetails=2996 / products=74, productlines=7
dim_oficina: 7  ·  dim_producto: 110  ·  dim_tiempo: 591
fact_ventas:  2647 filas cargadas
fact_servicio:  96 filas cargadas
STG: 22 registros cuarentenados → stg/registros_cuarentenados.parquet
✅ ETL completado exitosamente
```

### ETL 3 — Extensión del repositorio de metadatos (Entrega 2)

Registra la estructura del DW (tablas, dimensiones, hechos, proceso ETL) en las capas 4–5 del repositorio de metadatos.

```bash
docker compose run --rm etl_metadata_ext
```

Salida esperada:
```
✓ dw.fact_ventas (16 columnas)  ·  ✓ dw.fact_servicio (7 columnas)
✓ fact_ventas → dim_tiempo  ...  ✓ fact_servicio → dim_empleado_servicio
✓ etl_dw.py  (process_id=1)
✅ ETL de metadatos DW completado exitosamente
```

---

## 3. Verificar los datos cargados

```bash
# Contar filas por tabla del almacén
docker exec modelado_postgres psql -U admin -d datawarehouse -c "
SELECT tablename AS tabla,
       (xpath('/row/c/text()', query_to_xml(
           'SELECT COUNT(*) AS c FROM dw.' || tablename,
           false, true, '')))[1]::text::int AS filas
FROM pg_tables
WHERE schemaname = 'dw'
ORDER BY tablename;"
```

```bash
# Explorar registros cuarentenados (fuera del contenedor, en el host)
python3 -c "
import pandas as pd
df = pd.read_parquet('stg/registros_cuarentenados.parquet')
print(df[['tabla', 'pk_valor', 'columnas_afectadas']])"
```

---

## 4. Generar un backup

### Backup del almacén de datos

```bash
docker exec modelado_postgres pg_dump \
  -U admin -d datawarehouse \
  --format=custom --compress=9 \
  > backup/datawarehouse_$(date +%Y%m%d).dump
```

### Backup del repositorio de metadatos

```bash
docker exec modelado_postgres pg_dump \
  -U admin -d metadata_repository \
  --format=custom --compress=9 \
  > backup/metadata_repository_$(date +%Y%m%d).dump
```

> Los archivos `.dump` usan el formato binario de `pg_restore` (más eficiente que SQL plano). Crea la carpeta `backup/` antes si no existe: `mkdir -p backup`.

---

## 5. Restaurar un backup

### Preparar un contenedor limpio

```bash
# Eliminar volumen anterior y levantar postgres fresco
docker compose down -v
docker compose up -d postgres
```

### Restaurar el almacén de datos

```bash
docker exec -i modelado_postgres pg_restore \
  -U admin -d datawarehouse \
  --no-owner --role=admin \
  < backup/datawarehouse_YYYYMMDD.dump
```

### Restaurar el repositorio de metadatos

```bash
docker exec -i modelado_postgres pg_restore \
  -U admin -d metadata_repository \
  --no-owner --role=admin \
  < backup/metadata_repository_YYYYMMDD.dump
```

> Reemplaza `YYYYMMDD` con la fecha del archivo de backup que quieres restaurar.

---

## 6. Acceder a la base de datos manualmente

### Consola psql interactiva

```bash
# Almacén de datos
docker exec -it modelado_postgres psql -U admin -d datawarehouse

# Repositorio de metadatos
docker exec -it modelado_postgres psql -U admin -d metadata_repository
```

### Desde cualquier cliente externo (DBeaver, pgAdmin, TablePlus…)

| Parámetro | Valor               |
|-----------|---------------------|
| Host      | `localhost`         |
| Puerto    | `5433`              |
| Usuario   | `admin`             |
| Contraseña| `admin123`          |
| BD (DW)   | `datawarehouse`     |
| BD (meta) | `metadata_repository` |

### Ejecutar los reportes analíticos

```bash
docker exec -i modelado_postgres psql -U admin -d datawarehouse \
  < reports/reports.sql
```

---

## 7. Notebook de visualización

El notebook [reports/reportes_dw.ipynb](reports/reportes_dw.ipynb) conecta al DW y genera los cuatro reportes requeridos más cuatro análisis adicionales, todos con gráficos exportados como `.png`.

### Dependencias

```bash
pip3 install sqlalchemy psycopg2-binary pandas matplotlib seaborn numpy pyarrow
```

### Ejecutar

Con el contenedor corriendo (`docker compose up -d postgres`), abre el notebook en VS Code o con Jupyter:

```bash
jupyter notebook reports/reportes_dw.ipynb
```

Ejecuta todas las celdas en orden (`Run All`). Los gráficos se guardan automáticamente en `reports/`.

### Contenido

| Sección | Gráficos |
|---------|----------|
| **R1** Ventas por línea y trimestre | Barras apiladas por trimestre · Líneas de % margen por línea |
| **R2** Top 10 clientes | Barras horizontales por compras · Scatter compras vs. llamadas |
| **R3** Margen por territorio | Barras por representante coloreadas por territorio · Scatter ventas vs. % margen |
| **R4** Tendencia mensual | Serie dual: ventas (área) + llamadas de servicio (barras) |
| **A1** Top 15 productos | Barras horizontales por unidades vendidas, coloreadas por línea |
| **A2** Estacionalidad | Heatmap de ventas por mes × año |
| **A3** Clientes por país | Barras de ventas por país · Pie de distribución de clientes |
| **A4** Cuarentena | Registros no-ASCII por tabla y columna afectada |

---

## 8. Apagar

```bash
# Solo detener (conserva los datos en el volumen)
docker compose down

# Detener y eliminar el volumen (borra todos los datos)
docker compose down -v
```
# datawarehouse
