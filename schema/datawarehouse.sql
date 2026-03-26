-- ============================================================
-- DATA WAREHOUSE - CLASSICMODELS / CUSTOMERSERVICE
-- Esquema de constelación (Galaxy Schema)
-- Dos tablas de hechos: fact_ventas, fact_servicio
-- Dimensiones compartidas: dim_cliente, dim_producto, dim_tiempo
-- Encoding: UTF-8
-- ============================================================

SET client_encoding = 'UTF8';

CREATE SCHEMA IF NOT EXISTS dw;

-- ============================================================
-- DIMENSIÓN COMPARTIDA: dim_tiempo
-- Granularidad: un registro por día calendario
-- Fuente: generada a partir de fechas de órdenes y llamadas de servicio
-- Rango esperado: 2003-2005 (classicmodels) + fechas de cs_customer_calls
-- ============================================================
CREATE TABLE dw.dim_tiempo (
    tiempo_key      INTEGER     PRIMARY KEY,         -- YYYYMMDD como entero natural (ej: 20030106)
    fecha           DATE        NOT NULL,
    anio            SMALLINT    NOT NULL,
    trimestre       SMALLINT    NOT NULL,             -- 1-4
    mes             SMALLINT    NOT NULL,             -- 1-12
    nombre_mes      VARCHAR(20) NOT NULL,             -- 'Enero', 'Febrero', ...
    semana_anio     SMALLINT    NOT NULL,             -- 1-53 (ISO)
    dia_mes         SMALLINT    NOT NULL,             -- 1-31
    dia_semana      SMALLINT    NOT NULL,             -- 1=Lunes, 7=Domingo (ISO)
    nombre_dia      VARCHAR(20) NOT NULL,
    es_fin_semana   BOOLEAN     NOT NULL DEFAULT FALSE
);

-- ============================================================
-- DIMENSIÓN COMPARTIDA: dim_cliente
-- Fuente: classicmodels.customers  (customerName, creditLimit, ...)
--         customerservice.cs_customers (misma clave, sin customerName ni creditLimit)
-- Transformación: FULL OUTER JOIN por customerNumber; campos solo de classicmodels
--                 quedan NULL si el cliente solo existe en cs_customers
-- Integridad: 100% coincidencia en customerNumber (122 registros en ambas fuentes)
-- ============================================================
CREATE TABLE dw.dim_cliente (
    cliente_key         SERIAL          PRIMARY KEY,
    customer_number     INTEGER         NOT NULL UNIQUE,   -- clave natural de negocio
    customer_name       VARCHAR(50),                       -- solo en classicmodels.customers
    contact_last_name   VARCHAR(50),
    contact_first_name  VARCHAR(50),
    phone               VARCHAR(50),
    address_line1       VARCHAR(50),
    address_line2       VARCHAR(50),
    city                VARCHAR(50),
    state               VARCHAR(50),
    postal_code         VARCHAR(15),
    country             VARCHAR(50),
    credit_limit        DECIMAL(10,2)                      -- NULL si no hay info financiera
);

-- ============================================================
-- DIMENSIÓN COMPARTIDA: dim_producto
-- Fuente: classicmodels.products    (74 productos con buyPrice, MSRP, quantityInStock)
--         customerservice.cs_products (110 productos; superset — 36 solo en cs)
-- Transformación: FULL OUTER JOIN por productCode; campos financieros/inventario
--                 (buyPrice, MSRP, quantityInStock) quedan NULL para los 36 productos
--                 que solo existen en customerservice
-- ============================================================
CREATE TABLE dw.dim_producto (
    producto_key        SERIAL          PRIMARY KEY,
    product_code        VARCHAR(15)     NOT NULL UNIQUE,   -- clave natural de negocio
    product_name        VARCHAR(70),
    product_line        VARCHAR(50),                       -- NULL para productos solo en cs
    product_scale       VARCHAR(10),
    product_vendor      VARCHAR(50),
    buy_price           DECIMAL(10,2),                     -- NULL para productos solo en cs
    msrp                DECIMAL(10,2),                     -- NULL para productos solo en cs
    quantity_in_stock   SMALLINT,                          -- NULL para productos solo en cs
    fuente              VARCHAR(20)     NOT NULL            -- 'classicmodels' | 'customerservice' | 'ambas'
);

-- ============================================================
-- DIMENSIÓN EXCLUSIVA fact_ventas: dim_empleado_ventas
-- Fuente: classicmodels.employees (23 registros)
-- ============================================================
CREATE TABLE dw.dim_empleado_ventas (
    empleado_ventas_key INTEGER         PRIMARY KEY,       -- igual a employeeNumber
    employee_number     INTEGER         NOT NULL UNIQUE,
    last_name           VARCHAR(50),
    first_name          VARCHAR(50),
    email               VARCHAR(100),
    job_title           VARCHAR(50),
    office_code         VARCHAR(10)                        -- denormalizado para join con dim_oficina
);

-- ============================================================
-- DIMENSIÓN EXCLUSIVA fact_ventas: dim_oficina
-- Fuente: classicmodels.offices (7 registros — NA, EMEA, APAC, Japan)
-- ============================================================
CREATE TABLE dw.dim_oficina (
    oficina_key     SERIAL          PRIMARY KEY,
    office_code     VARCHAR(10)     NOT NULL UNIQUE,
    city            VARCHAR(50),
    state           VARCHAR(50),
    country         VARCHAR(50),
    territory       VARCHAR(10)                            -- 'NA', 'EMEA', 'APAC', 'Japan'
);

-- ============================================================
-- DIMENSIÓN EXCLUSIVA fact_ventas: dim_linea_producto
-- Fuente: classicmodels.productlines (7 categorías)
-- Nota: htmlDescription e image excluidos — 100% NULL en la fuente
--       (problema de calidad identificado en Entrega 1)
-- ============================================================
CREATE TABLE dw.dim_linea_producto (
    linea_producto_key  SERIAL          PRIMARY KEY,
    product_line        VARCHAR(50)     NOT NULL UNIQUE,
    text_description    VARCHAR(4000)
);

-- ============================================================
-- DIMENSIÓN EXCLUSIVA fact_servicio: dim_empleado_servicio
-- Fuente: customerservice.cs_employees (15 agentes de call center)
-- Nota: población diferente a dim_empleado_ventas (0% overlap — departamentos separados)
-- ============================================================
CREATE TABLE dw.dim_empleado_servicio (
    empleado_servicio_key   INTEGER         PRIMARY KEY,   -- igual a employeenumber
    employee_number         INTEGER         NOT NULL UNIQUE,
    last_name               VARCHAR(50),
    first_name              VARCHAR(50),
    email                   VARCHAR(100)
);

-- ============================================================
-- TABLA DE HECHOS: fact_ventas
-- Granularidad: UNA FILA POR LÍNEA DE ORDEN (orderdetails)
-- Fuentes:
--   classicmodels.orderdetails  → medidas principales
--   classicmodels.orders        → fecha, estado, cliente
--   classicmodels.customers     → resuelve dim_cliente
--   classicmodels.products      → resuelve dim_producto y dim_linea_producto
--   classicmodels.employees     → resuelve dim_empleado_ventas (via salesRepEmployeeNumber)
--   classicmodels.offices       → resuelve dim_oficina (via employees.officeCode)
-- Volumen esperado: ~2,996 filas
-- ============================================================
CREATE TABLE dw.fact_ventas (
    fact_ventas_key         SERIAL          PRIMARY KEY,

    -- Claves foráneas a dimensiones
    tiempo_key              INTEGER         NOT NULL REFERENCES dw.dim_tiempo(tiempo_key),
    cliente_key             INTEGER         NOT NULL REFERENCES dw.dim_cliente(cliente_key),
    producto_key            INTEGER         NOT NULL REFERENCES dw.dim_producto(producto_key),
    empleado_ventas_key     INTEGER         REFERENCES dw.dim_empleado_ventas(empleado_ventas_key),  -- NULL si cliente sin rep asignado
    oficina_key             INTEGER         REFERENCES dw.dim_oficina(oficina_key),
    linea_producto_key      INTEGER         NOT NULL REFERENCES dw.dim_linea_producto(linea_producto_key),

    -- Dimensiones degeneradas (identificadores de la transacción sin tabla propia)
    order_number            INTEGER         NOT NULL,
    order_line_number       SMALLINT        NOT NULL,
    order_status            VARCHAR(15),

    -- Medidas
    cantidad_ordenada       INTEGER         NOT NULL,       -- orderdetails.quantityOrdered
    precio_unitario         DECIMAL(10,2)   NOT NULL,       -- orderdetails.priceEach (precio negociado)
    monto_linea             DECIMAL(12,2)   NOT NULL,       -- cantidad_ordenada * precio_unitario
    precio_compra           DECIMAL(10,2),                  -- products.buyPrice (costo unitario)
    margen_linea            DECIMAL(12,2),                  -- monto_linea - (cantidad_ordenada * precio_compra)
    msrp                    DECIMAL(10,2)                   -- products.MSRP (precio sugerido de catálogo)
);

-- ============================================================
-- TABLA DE HECHOS: fact_servicio
-- Granularidad: UNA FILA POR LLAMADA DE SERVICIO (cs_customer_calls)
-- Fuentes:
--   customerservice.cs_customer_calls → base del hecho (employeenumber, customernumber, productcode, date, text)
--   customerservice.cs_customers      → resuelve dim_cliente
--   customerservice.cs_employees      → resuelve dim_empleado_servicio
--   customerservice.cs_products       → resuelve dim_producto
-- Nota: cs_customer_calls no registra duración ni motivo estructurado;
--       solo texto libre (campo text) — limitación de la fuente de datos
-- ============================================================
CREATE TABLE dw.fact_servicio (
    fact_servicio_key       SERIAL      PRIMARY KEY,

    -- Claves foráneas a dimensiones
    tiempo_key              INTEGER     NOT NULL REFERENCES dw.dim_tiempo(tiempo_key),
    cliente_key             INTEGER     NOT NULL REFERENCES dw.dim_cliente(cliente_key),
    producto_key            INTEGER     NOT NULL REFERENCES dw.dim_producto(producto_key),
    empleado_servicio_key   INTEGER     NOT NULL REFERENCES dw.dim_empleado_servicio(empleado_servicio_key),

    -- Medidas
    conteo_llamadas         SMALLINT    NOT NULL DEFAULT 1,         -- siempre 1; permite SUM en agregaciones
    tiene_notas             BOOLEAN     NOT NULL DEFAULT FALSE       -- TRUE si cs_customer_calls.text IS NOT NULL
);

-- ============================================================
-- ÍNDICES para performance en consultas analíticas (slice & dice)
-- ============================================================

-- fact_ventas
CREATE INDEX idx_fv_tiempo      ON dw.fact_ventas(tiempo_key);
CREATE INDEX idx_fv_cliente     ON dw.fact_ventas(cliente_key);
CREATE INDEX idx_fv_producto    ON dw.fact_ventas(producto_key);
CREATE INDEX idx_fv_empleado    ON dw.fact_ventas(empleado_ventas_key);
CREATE INDEX idx_fv_oficina     ON dw.fact_ventas(oficina_key);
CREATE INDEX idx_fv_linea       ON dw.fact_ventas(linea_producto_key);
CREATE INDEX idx_fv_order       ON dw.fact_ventas(order_number);

-- fact_servicio
CREATE INDEX idx_fs_tiempo      ON dw.fact_servicio(tiempo_key);
CREATE INDEX idx_fs_cliente     ON dw.fact_servicio(cliente_key);
CREATE INDEX idx_fs_producto    ON dw.fact_servicio(producto_key);
CREATE INDEX idx_fs_empleado    ON dw.fact_servicio(empleado_servicio_key);
