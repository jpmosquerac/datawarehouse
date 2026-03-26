#!/usr/bin/env python3
"""
ETL — Almacén de Datos (Data Warehouse)
Modelos y Persistencia de Datos 2026-01 — Entrega 2

Lee datos directamente de los archivos .sql de las fuentes:
  - mysqlsampledatabase.sql  →  classicmodels  (INSERT multi-fila MySQL)
  - customerservice.sql      →  customerservice (COPY tab-separated PostgreSQL)

Fases:
  1. Parseo   — extrae filas en estructuras Python sin cargar los dumps en BD
  2. Dims     — carga las 7 dimensiones (orden respeta FK)
  3. Hechos   — carga fact_ventas (~2,996 filas) y fact_servicio

Ejecución:
  python etl_dw.py
  DW_HOST=... DW_PORT=5434 DW_DB=datawarehouse python etl_dw.py
"""

import re
import os
import json
import logging
from datetime import date

import pandas as pd
import psycopg2

# ─── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Configuración ─────────────────────────────────────────────────────────
DW_CONFIG = {
    "host":     os.getenv("DW_HOST",     "localhost"),
    "port":     int(os.getenv("DW_PORT", "5433")),
    "dbname":   os.getenv("DW_DB",       "datawarehouse"),
    "user":     os.getenv("DW_USER",     "admin"),
    "password": os.getenv("DW_PASSWORD", "admin123"),
}

_BASE    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.getenv(
    "DATA_SOURCES_DIR",
    os.path.join(_BASE, "../../metadata_repo/data_sources"),
)
MYSQL_FILE = os.path.join(DATA_DIR, "mysqlsampledatabase.sql")
PG_FILE    = os.path.join(DATA_DIR, "customerservice.sql")
STG_DIR    = os.path.join(_BASE, "../stg")

# Acumula registros cuarentenados durante la ejecución → se vuelcan a Parquet al final
_stg_records: list[dict] = []

MESES = [
    "", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]
DIAS = ["", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]


# ══════════════════════════════════════════════════════════════════════════
# CUARENTENA STG — Detección de caracteres no-ASCII antes del ETL
# ══════════════════════════════════════════════════════════════════════════

def _quarantine(row: dict, tabla: str, pk_col: str) -> bool:
    """
    Detecta si algún campo de texto del registro contiene caracteres no-ASCII
    (originados en el charset latin-1 del dump MySQL).

    Si encuentra caracteres problemáticos:
      1. Guarda el registro completo (sin tocar) en _stg_records.
      2. Retorna True  → el llamador debe hacer `continue` (omitir el registro).

    Si el registro es ASCII-seguro:
      Retorna False → el llamador puede insertarlo normalmente en el DW.
    """
    offending = [
        col for col, val in row.items()
        if isinstance(val, str) and not val.isascii()
    ]
    if not offending:
        return False

    _stg_records.append({
        "tabla":              tabla,
        "pk_valor":           str(row.get(pk_col, "")),
        "columnas_afectadas": ", ".join(offending),
        "registro_json":      json.dumps(row, ensure_ascii=False),
        "fecha_carga":        pd.Timestamp.now(),
    })
    return True


def _save_stg() -> None:
    """Vuelca _stg_records a stg/registros_cuarentenados.parquet."""
    if not _stg_records:
        log.info("STG: ningún registro cuarentenado.")
        return
    os.makedirs(STG_DIR, exist_ok=True)
    path = os.path.join(STG_DIR, "registros_cuarentenados.parquet")
    pd.DataFrame(_stg_records).to_parquet(path, index=False, engine="pyarrow")
    log.info(f"STG: {len(_stg_records)} registros cuarentenados → {path}")


# ══════════════════════════════════════════════════════════════════════════
# FASE 1 — PARSERS
# ══════════════════════════════════════════════════════════════════════════

def _parse_value_tuples(values_block: str) -> list[list]:
    """
    Parsea el bloque VALUES de un INSERT MySQL en lista de listas (strings o None).
    Maneja: NULL, enteros, decimales, strings con comas, escape \\' y ''.
    """
    rows, row, val = [], [], []
    in_str = False
    depth  = 0
    i      = 0
    n      = len(values_block)

    while i < n:
        ch = values_block[i]

        if in_str:
            # Escaped quote: \' o ''
            if ch == "\\" and i + 1 < n and values_block[i + 1] == "'":
                val.append("'"); i += 2; continue
            if ch == "'" and i + 1 < n and values_block[i + 1] == "'":
                val.append("'"); i += 2; continue
            # Demás escapes MySQL (\n, \r, \t, \\)
            if ch == "\\" and i + 1 < n:
                esc = values_block[i + 1]
                val.append({"n": "\n", "r": "\r", "t": "\t", "\\": "\\"}.get(esc, esc))
                i += 2; continue
            if ch == "'":
                in_str = False
            else:
                val.append(ch)
        else:
            if ch == "(" and depth == 0:
                depth, row, val = 1, [], []
            elif ch == ")" and depth == 1:
                v = "".join(val).strip()
                row.append(None if v.upper() == "NULL" else v)
                rows.append(row)
                row, val, depth = [], [], 0
            elif ch == "," and depth == 1:
                v = "".join(val).strip()
                row.append(None if v.upper() == "NULL" else v)
                val = []
            elif ch == "'" and depth == 1:
                in_str = True
            elif depth == 1:
                val.append(ch)
        i += 1

    return rows


def parse_mysql_table(sql: str, table: str) -> list[dict]:
    """
    Extrae filas del INSERT de una tabla MySQL como lista de dicts.
    Soporta INSERTs multi-fila: insert into `t` (cols) values (...),(...),...;

    El regex solo captura la cabecera (nombres de columna). El bloque VALUES
    se delimita hasta la próxima sentencia SQL de nivel superior para evitar
    que el ';' no-greedy corte dentro de descripciones con punto y coma.
    """
    m = re.search(
        rf"insert\s+into\s+`{re.escape(table)}`\s*\(([^)]+)\)\s*values\s*",
        sql,
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        log.warning(f"No INSERT encontrado para tabla MySQL: {table}")
        return []

    cols = [c.strip().strip("`") for c in m.group(1).split(",")]

    # Delimitamos el bloque VALUES hasta la próxima sentencia de nivel superior.
    # Estos tokens nunca aparecen dentro de las tuplas de datos.
    rest = sql[m.end():]
    boundary = re.search(
        r"\n(?:UNLOCK|ALTER\s+TABLE|CREATE|DROP|INSERT\s+INTO|/\*!)",
        rest, re.IGNORECASE,
    )
    values_block = rest[:boundary.start()] if boundary else rest

    rows = [dict(zip(cols, r)) for r in _parse_value_tuples(values_block)]
    log.debug(f"  MySQL {table}: {len(rows)} filas, columnas={cols}")
    return rows


def parse_pg_copy(sql: str, table: str) -> list[dict]:
    """
    Extrae filas de un bloque COPY ... FROM stdin del dump PostgreSQL.
    Formato: tab-separated, \\N = NULL, terminado por \\. en línea propia.
    """
    m = re.search(
        rf"COPY\s+(?:public\.)?{re.escape(table)}\s*\(([^)]+)\)\s*FROM\s+stdin;\n",
        sql,
        re.IGNORECASE,
    )
    if not m:
        log.warning(f"No COPY encontrado para tabla PostgreSQL: {table}")
        return []

    cols       = [c.strip() for c in m.group(1).split(",")]
    data_start = m.end()
    data_block = sql[data_start:]

    # Terminator \.  al inicio de una línea
    term = re.search(r"^\\\.", data_block, re.MULTILINE)
    if term:
        data_block = data_block[: term.start()]

    rows = []
    for line in data_block.splitlines():
        if line:
            vals = [None if v == "\\N" else v for v in line.split("\t")]
            rows.append(dict(zip(cols, vals)))

    log.debug(f"  PG COPY {table}: {len(rows)} filas, columnas={cols}")
    return rows


# ─── Helpers de conversión de tipo ─────────────────────────────────────────

def _d(s) -> date:
    """Convierte string 'YYYY-MM-DD' o 'YYYY-MM-DD HH:MM:SS' a date."""
    return date.fromisoformat(str(s)[:10])

def _int(v) -> int | None:
    return int(v) if v is not None else None

def _float(v) -> float | None:
    return float(v) if v is not None else None


# ══════════════════════════════════════════════════════════════════════════
# FASE 2 — DIMENSIONES
# ══════════════════════════════════════════════════════════════════════════

def load_dim_oficina(cur, offices: list[dict]) -> dict[str, int]:
    """Retorna {office_code → oficina_key}"""
    result = {}
    for o in offices:
        if _quarantine(o, "offices", "officeCode"):
            continue
        cur.execute(
            """
            INSERT INTO dw.dim_oficina (office_code, city, state, country, territory)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (office_code) DO UPDATE
              SET city=EXCLUDED.city, state=EXCLUDED.state,
                  country=EXCLUDED.country, territory=EXCLUDED.territory
            RETURNING oficina_key
            """,
            (o["officeCode"], o["city"], o.get("state"), o["country"], o["territory"]),
        )
        result[o["officeCode"]] = cur.fetchone()[0]
    log.info(f"  dim_oficina:         {len(result):>5} filas")
    return result


def load_dim_linea_producto(cur, productlines: list[dict]) -> dict[str, int]:
    """Retorna {product_line → linea_producto_key}"""
    result = {}
    for pl in productlines:
        if _quarantine(pl, "productlines", "productLine"):
            continue
        cur.execute(
            """
            INSERT INTO dw.dim_linea_producto (product_line, text_description)
            VALUES (%s, %s)
            ON CONFLICT (product_line) DO UPDATE
              SET text_description=EXCLUDED.text_description
            RETURNING linea_producto_key
            """,
            (pl["productLine"], pl.get("textDescription")),
        )
        result[pl["productLine"]] = cur.fetchone()[0]
    log.info(f"  dim_linea_producto:  {len(result):>5} filas")
    return result


def load_dim_empleado_ventas(cur, employees: list[dict]) -> dict[int, int]:
    """Retorna {employee_number → empleado_ventas_key}"""
    result = {}
    for e in employees:
        if _quarantine(e, "employees", "employeeNumber"):
            continue
        n = int(e["employeeNumber"])
        cur.execute(
            """
            INSERT INTO dw.dim_empleado_ventas
              (empleado_ventas_key, employee_number, last_name, first_name,
               email, job_title, office_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (employee_number) DO UPDATE
              SET last_name=EXCLUDED.last_name, first_name=EXCLUDED.first_name,
                  email=EXCLUDED.email, job_title=EXCLUDED.job_title,
                  office_code=EXCLUDED.office_code
            RETURNING empleado_ventas_key
            """,
            (n, n, e["lastName"], e["firstName"], e["email"], e["jobTitle"], e["officeCode"]),
        )
        result[n] = cur.fetchone()[0]
    log.info(f"  dim_empleado_ventas: {len(result):>5} filas")
    return result


def load_dim_empleado_servicio(cur, cs_employees: list[dict]) -> dict[int, int]:
    """Retorna {employee_number → empleado_servicio_key}"""
    result = {}
    for e in cs_employees:
        if _quarantine(e, "cs_employees", "employeenumber"):
            continue
        n = int(e["employeenumber"])
        cur.execute(
            """
            INSERT INTO dw.dim_empleado_servicio
              (empleado_servicio_key, employee_number, last_name, first_name, email)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (employee_number) DO UPDATE
              SET last_name=EXCLUDED.last_name, first_name=EXCLUDED.first_name,
                  email=EXCLUDED.email
            RETURNING empleado_servicio_key
            """,
            (n, n, e.get("lastname"), e.get("firstname"), e.get("email")),
        )
        result[n] = cur.fetchone()[0]
    log.info(f"  dim_empleado_serv:   {len(result):>5} filas")
    return result


def load_dim_cliente(
    cur, customers: list[dict], cs_customers: list[dict]
) -> dict[int, int]:
    """
    Merge classicmodels.customers + cs_customers por customerNumber.
    100% coincidencia en ambas fuentes (122 registros).
    Retorna {customer_number → cliente_key}.
    """
    cm_idx = {int(c["customerNumber"]): c for c in customers}
    cs_idx = {int(c["customernumber"]): c for c in cs_customers}
    result = {}

    for num in sorted(set(cm_idx) | set(cs_idx)):
        cm = cm_idx.get(num, {})
        cs = cs_idx.get(num, {})

        # Construir registro fusionado para poder inspeccionarlo antes de insertar
        merged = {
            "customer_number":     num,
            "customer_name":       cm.get("customerName"),
            "contact_last_name":   cm.get("contactLastName")  or cs.get("contactlastname"),
            "contact_first_name":  cm.get("contactFirstName") or cs.get("contactfirstname"),
            "phone":               cm.get("phone")            or cs.get("phone"),
            "address_line1":       cm.get("addressLine1")     or cs.get("addressline1"),
            "address_line2":       cm.get("addressLine2")     or cs.get("addressline2"),
            "city":                cm.get("city")             or cs.get("city"),
            "state":               cm.get("state")            or cs.get("state"),
            "postal_code":         cm.get("postalCode")       or cs.get("postalcode"),
            "country":             cm.get("country")          or cs.get("country"),
        }
        if _quarantine(merged, "customers", "customer_number"):
            continue

        cur.execute(
            """
            INSERT INTO dw.dim_cliente (
              customer_number, customer_name, contact_last_name, contact_first_name,
              phone, address_line1, address_line2, city, state, postal_code,
              country, credit_limit
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (customer_number) DO UPDATE SET
              customer_name=EXCLUDED.customer_name,
              contact_last_name=EXCLUDED.contact_last_name,
              contact_first_name=EXCLUDED.contact_first_name,
              phone=EXCLUDED.phone, address_line1=EXCLUDED.address_line1,
              address_line2=EXCLUDED.address_line2, city=EXCLUDED.city,
              state=EXCLUDED.state, postal_code=EXCLUDED.postal_code,
              country=EXCLUDED.country, credit_limit=EXCLUDED.credit_limit
            RETURNING cliente_key
            """,
            (
                num,
                merged["customer_name"],    merged["contact_last_name"],
                merged["contact_first_name"], merged["phone"],
                merged["address_line1"],    merged["address_line2"],
                merged["city"],             merged["state"],
                merged["postal_code"],      merged["country"],
                _float(cm.get("creditLimit")),
            ),
        )
        result[num] = cur.fetchone()[0]

    log.info(f"  dim_cliente:         {len(result):>5} filas")
    return result


def load_dim_producto(
    cur, products: list[dict], cs_products: list[dict]
) -> dict[str, int]:
    """
    Merge classicmodels.products (74) + cs_products (110) por productCode.
    cs_products es superconjunto — 36 productos solo en customerservice.
    Retorna {product_code → producto_key}.
    """
    cm_idx = {p["productCode"]: p for p in products}
    cs_idx = {p["productcode"]: p for p in cs_products}
    result = {}

    for code in sorted(set(cm_idx) | set(cs_idx)):
        cm = cm_idx.get(code)
        cs = cs_idx.get(code)
        fuente = "ambas" if (cm and cs) else ("classicmodels" if cm else "customerservice")

        merged = {
            "product_code":   code,
            "product_name":   cm["productName"]   if cm else cs.get("productname"),
            "product_line":   cm["productLine"]   if cm else None,
            "product_scale":  cm["productScale"]  if cm else cs.get("productscale"),
            "product_vendor": cm["productVendor"] if cm else cs.get("productvendor"),
        }
        if _quarantine(merged, "products", "product_code"):
            continue

        cur.execute(
            """
            INSERT INTO dw.dim_producto (
              product_code, product_name, product_line, product_scale,
              product_vendor, buy_price, msrp, quantity_in_stock, fuente
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (product_code) DO UPDATE SET
              product_name=EXCLUDED.product_name, product_line=EXCLUDED.product_line,
              product_scale=EXCLUDED.product_scale, product_vendor=EXCLUDED.product_vendor,
              buy_price=EXCLUDED.buy_price, msrp=EXCLUDED.msrp,
              quantity_in_stock=EXCLUDED.quantity_in_stock, fuente=EXCLUDED.fuente
            RETURNING producto_key
            """,
            (
                code,
                merged["product_name"], merged["product_line"],
                merged["product_scale"], merged["product_vendor"],
                _float(cm["buyPrice"]) if cm else None,
                _float(cm["MSRP"]) if cm else None,
                _int(cm["quantityInStock"]) if cm else None,
                fuente,
            ),
        )
        result[code] = cur.fetchone()[0]

    cs_only = sum(1 for c in result if c not in cm_idx)
    log.info(f"  dim_producto:        {len(result):>5} filas  (ambas={len(cm_idx)}, cs_only={cs_only})")
    return result


def load_dim_tiempo(
    cur, orders: list[dict], calls: list[dict]
) -> dict[int, int]:
    """
    Genera dim_tiempo a partir de todas las fechas únicas en órdenes y llamadas.
    Retorna {YYYYMMDD → YYYYMMDD} (tiempo_key es la propia fecha entero).
    """
    dates: set[date] = set()
    for o in orders:
        for field in ("orderDate", "requiredDate", "shippedDate"):
            if o.get(field):
                dates.add(_d(o[field]))
    for c in calls:
        if c.get("date"):
            dates.add(_d(c["date"]))

    result = {}
    for d in sorted(dates):
        key = int(d.strftime("%Y%m%d"))
        wd  = d.isoweekday()  # 1=Lunes, 7=Domingo
        cur.execute(
            """
            INSERT INTO dw.dim_tiempo (
              tiempo_key, fecha, anio, trimestre, mes, nombre_mes,
              semana_anio, dia_mes, dia_semana, nombre_dia, es_fin_semana
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (tiempo_key) DO NOTHING
            """,
            (
                key, d,
                d.year,
                (d.month - 1) // 3 + 1,
                d.month, MESES[d.month],
                d.isocalendar()[1],
                d.day, wd, DIAS[wd],
                wd >= 6,
            ),
        )
        result[key] = key

    log.info(f"  dim_tiempo:          {len(result):>5} fechas únicas")
    return result


# ══════════════════════════════════════════════════════════════════════════
# FASE 3 — HECHOS
# ══════════════════════════════════════════════════════════════════════════

def load_fact_ventas(
    cur,
    orderdetails: list[dict],
    orders: list[dict],
    products: list[dict],
    customers: list[dict],
    employees: list[dict],
    dim_cliente: dict,
    dim_producto: dict,
    dim_empleado_ventas: dict,
    dim_oficina: dict,
    dim_linea_producto: dict,
) -> int:
    """
    Granularidad: una fila por línea de orden (orderdetails).
    Joins en memoria: orderdetails → orders → customers → employees → offices
                                             → products → productlines
    """
    orders_idx    = {int(o["orderNumber"]): o for o in orders}
    customers_idx = {int(c["customerNumber"]): c for c in customers}
    employees_idx = {int(e["employeeNumber"]): e for e in employees}
    products_idx  = {p["productCode"]: p for p in products}

    ok = skip = 0
    for od in orderdetails:
        order_num = int(od["orderNumber"])
        prod_code = od["productCode"]
        order     = orders_idx.get(order_num)
        product   = products_idx.get(prod_code)

        if not order or not product:
            log.warning(f"    Datos faltantes orden={order_num} prod={prod_code}")
            skip += 1; continue

        cust_num   = int(order["customerNumber"])
        tiempo_key = int(_d(order["orderDate"]).strftime("%Y%m%d"))

        cliente_key  = dim_cliente.get(cust_num)
        producto_key = dim_producto.get(prod_code)
        linea_key    = dim_linea_producto.get(product["productLine"])

        if not cliente_key or not producto_key or not linea_key:
            log.warning(f"    FK sin resolver: cust={cust_num} prod={prod_code} linea={product.get('productLine')}")
            skip += 1; continue

        # Empleado de ventas (puede ser NULL — clientes sin rep asignado)
        customer = customers_idx.get(cust_num, {})
        emp_num  = _int(customer.get("salesRepEmployeeNumber"))
        emp_key  = dim_empleado_ventas.get(emp_num) if emp_num else None

        # Oficina (via empleado)
        oficina_key = None
        if emp_num:
            emp = employees_idx.get(emp_num, {})
            oficina_key = dim_oficina.get(emp.get("officeCode"))

        # Medidas
        qty    = int(od["quantityOrdered"])
        price  = float(od["priceEach"])
        monto  = round(qty * price, 2)
        buy    = float(product["buyPrice"])
        margen = round(monto - qty * buy, 2)
        msrp   = float(product["MSRP"])

        cur.execute(
            """
            INSERT INTO dw.fact_ventas (
              tiempo_key, cliente_key, producto_key, empleado_ventas_key,
              oficina_key, linea_producto_key,
              order_number, order_line_number, order_status,
              cantidad_ordenada, precio_unitario, monto_linea,
              precio_compra, margen_linea, msrp
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                tiempo_key, cliente_key, producto_key, emp_key,
                oficina_key, linea_key,
                order_num, int(od["orderLineNumber"]), order.get("status"),
                qty, price, monto, buy, margen, msrp,
            ),
        )
        ok += 1

    log.info(f"  fact_ventas:         {ok:>5} filas cargadas  ({skip} omitidas)")
    return ok


def load_fact_servicio(
    cur,
    calls: list[dict],
    dim_cliente: dict,
    dim_producto: dict,
    dim_empleado_servicio: dict,
) -> int:
    """
    Granularidad: una fila por llamada de servicio (cs_customer_calls).
    Nota: cs_customer_calls no registra duración ni motivo estructurado;
    medidas disponibles: conteo_llamadas (=1) y tiene_notas (text IS NOT NULL).
    """
    ok = skip = 0
    for c in calls:
        emp_num   = _int(c.get("employeenumber"))
        cust_num  = _int(c.get("customernumber"))
        prod_code = c.get("productcode")
        call_date = _d(c["date"]) if c.get("date") else None

        if not call_date:
            skip += 1; continue

        tiempo_key = int(call_date.strftime("%Y%m%d"))
        emp_key    = dim_empleado_servicio.get(emp_num)
        cust_key   = dim_cliente.get(cust_num)
        prod_key   = dim_producto.get(prod_code)

        if not emp_key or not cust_key or not prod_key:
            log.warning(f"    FK sin resolver: emp={emp_num} cust={cust_num} prod={prod_code}")
            skip += 1; continue

        cur.execute(
            """
            INSERT INTO dw.fact_servicio (
              tiempo_key, cliente_key, producto_key, empleado_servicio_key,
              conteo_llamadas, tiene_notas
            ) VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (tiempo_key, cust_key, prod_key, emp_key, 1, c.get("text") is not None),
        )
        ok += 1

    log.info(f"  fact_servicio:       {ok:>5} filas cargadas  ({skip} omitidas)")
    return ok


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    # ── 1. Leer archivos fuente ───────────────────────────────────────────
    # mysqlsampledatabase.sql usa DEFAULT CHARSET=latin1 → se decodifica a Unicode
    # Se decodifica a Unicode; registros con caracteres no-ASCII van a Parquet (stg/).
    log.info("Leyendo archivos fuente...")
    with open(MYSQL_FILE, encoding="latin-1") as f:
        mysql_sql = f.read()
    with open(PG_FILE, encoding="utf-8") as f:
        pg_sql = f.read()

    # ── 2. Parsear tablas MySQL (classicmodels) ───────────────────────────
    log.info("Parseando classicmodels (MySQL INSERT)...")
    customers    = parse_mysql_table(mysql_sql, "customers")
    employees    = parse_mysql_table(mysql_sql, "employees")
    offices      = parse_mysql_table(mysql_sql, "offices")
    orders       = parse_mysql_table(mysql_sql, "orders")
    orderdetails = parse_mysql_table(mysql_sql, "orderdetails")
    products     = parse_mysql_table(mysql_sql, "products")
    productlines = parse_mysql_table(mysql_sql, "productlines")

    log.info(f"  customers={len(customers)}, employees={len(employees)}, offices={len(offices)}")
    log.info(f"  orders={len(orders)}, orderdetails={len(orderdetails)}")
    log.info(f"  products={len(products)}, productlines={len(productlines)}")

    # ── 3. Parsear tablas PostgreSQL (customerservice COPY) ───────────────
    log.info("Parseando customerservice (PostgreSQL COPY)...")
    cs_customers = parse_pg_copy(pg_sql, "cs_customers")
    cs_employees = parse_pg_copy(pg_sql, "cs_employees")
    cs_calls     = parse_pg_copy(pg_sql, "cs_customer_calls")
    cs_products  = parse_pg_copy(pg_sql, "cs_products")

    log.info(f"  cs_customers={len(cs_customers)}, cs_employees={len(cs_employees)}")
    log.info(f"  cs_calls={len(cs_calls)}, cs_products={len(cs_products)}")

    # ── 4. Conectar al almacén ────────────────────────────────────────────
    log.info(f"Conectando a {DW_CONFIG['host']}:{DW_CONFIG['port']}/{DW_CONFIG['dbname']}...")
    conn = psycopg2.connect(**DW_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # ── 5. Dimensiones (orden respeta FK entre tablas) ────────────────
        log.info("Cargando dimensiones...")
        dim_oficina         = load_dim_oficina(cur, offices)
        dim_linea_producto  = load_dim_linea_producto(cur, productlines)
        dim_emp_ventas      = load_dim_empleado_ventas(cur, employees)
        dim_emp_servicio    = load_dim_empleado_servicio(cur, cs_employees)
        dim_cliente         = load_dim_cliente(cur, customers, cs_customers)
        dim_producto        = load_dim_producto(cur, products, cs_products)
        dim_tiempo          = load_dim_tiempo(cur, orders, cs_calls)

        # ── 6. Hechos ─────────────────────────────────────────────────────
        log.info("Cargando hechos...")
        load_fact_ventas(
            cur, orderdetails, orders, products, customers, employees,
            dim_cliente, dim_producto, dim_emp_ventas,
            dim_oficina, dim_linea_producto,
        )
        load_fact_servicio(
            cur, cs_calls, dim_cliente, dim_producto, dim_emp_servicio,
        )

        conn.commit()
        log.info("ETL completado exitosamente.")
        _save_stg()

    except Exception as exc:
        conn.rollback()
        log.error(f"Error — se hizo rollback: {exc}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
