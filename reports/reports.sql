-- ============================================================
-- REPORTES ANALÍTICOS — Data Warehouse ClassicModels/CustomerService
-- Conectan a dw.fact_ventas y dw.fact_servicio (no a fuentes originales)
-- ============================================================

-- ============================================================
-- REPORTE 1: Ventas por línea de producto y trimestre
-- Dimensiones: dim_tiempo, dim_linea_producto
-- Hecho: fact_ventas
-- ============================================================

SELECT
    dt.anio,
    dt.trimestre,
    dlp.product_line                                                        AS linea_producto,
    COUNT(DISTINCT fv.order_number)                                         AS num_ordenes,
    SUM(fv.cantidad_ordenada)                                               AS unidades_vendidas,
    ROUND(SUM(fv.monto_linea)::NUMERIC, 2)                                  AS total_ventas,
    ROUND(SUM(fv.margen_linea)::NUMERIC, 2)                                 AS margen_bruto,
    ROUND(
        SUM(fv.margen_linea) / NULLIF(SUM(fv.monto_linea), 0) * 100
    , 2)                                                                    AS pct_margen
FROM dw.fact_ventas fv
JOIN dw.dim_tiempo          dt  ON dt.tiempo_key        = fv.tiempo_key
JOIN dw.dim_linea_producto  dlp ON dlp.linea_producto_key = fv.linea_producto_key
GROUP BY dt.anio, dt.trimestre, dlp.product_line
ORDER BY dt.anio, dt.trimestre, total_ventas DESC;


-- ============================================================
-- REPORTE 2: Top 10 clientes por volumen de compra
--            con comparación de llamadas de servicio recibidas
-- Dimensiones: dim_cliente (compartida entre ambos hechos)
-- Hechos: fact_ventas + fact_servicio
-- ============================================================

SELECT
    dc.customer_name                                                        AS cliente,
    dc.country                                                              AS pais,
    COUNT(DISTINCT fv.order_number)                                         AS num_ordenes,
    SUM(fv.cantidad_ordenada)                                               AS unidades_compradas,
    ROUND(SUM(fv.monto_linea)::NUMERIC, 2)                                  AS total_compras,
    COALESCE(s.num_llamadas, 0)                                             AS llamadas_servicio,
    ROUND(
        COALESCE(s.num_llamadas, 0)::NUMERIC /
        NULLIF(COUNT(DISTINCT fv.order_number), 0)
    , 2)                                                                    AS llamadas_por_orden
FROM dw.fact_ventas fv
JOIN dw.dim_cliente dc ON dc.cliente_key = fv.cliente_key
LEFT JOIN (
    SELECT cliente_key, COUNT(*) AS num_llamadas
    FROM dw.fact_servicio
    GROUP BY cliente_key
) s ON s.cliente_key = dc.cliente_key
GROUP BY dc.cliente_key, dc.customer_name, dc.country, s.num_llamadas
ORDER BY total_compras DESC
LIMIT 10;


-- ============================================================
-- REPORTE 3: Margen por territorio y representante de ventas
-- Dimensiones: dim_oficina, dim_empleado_ventas
-- Hecho: fact_ventas
-- ============================================================

SELECT
    dof.territory                                                           AS territorio,
    dof.country                                                             AS pais_oficina,
    dev.first_name || ' ' || dev.last_name                                  AS representante,
    dev.job_title                                                           AS cargo,
    COUNT(DISTINCT fv.order_number)                                         AS num_ordenes,
    COUNT(DISTINCT fv.cliente_key)                                          AS num_clientes,
    ROUND(SUM(fv.monto_linea)::NUMERIC, 2)                                  AS total_ventas,
    ROUND(SUM(fv.margen_linea)::NUMERIC, 2)                                 AS margen_total,
    ROUND(
        SUM(fv.margen_linea) / NULLIF(SUM(fv.monto_linea), 0) * 100
    , 2)                                                                    AS pct_margen
FROM dw.fact_ventas fv
JOIN dw.dim_empleado_ventas dev ON dev.empleado_ventas_key = fv.empleado_ventas_key
JOIN dw.dim_oficina         dof ON dof.oficina_key         = fv.oficina_key
GROUP BY
    dof.territory,
    dof.country,
    dev.empleado_ventas_key,
    dev.first_name,
    dev.last_name,
    dev.job_title
ORDER BY dof.territory, margen_total DESC;


-- ============================================================
-- REPORTE 4: Tendencia mensual — ventas vs llamadas de servicio
-- Dimensión compartida: dim_tiempo
-- Hechos: fact_ventas + fact_servicio
-- Útil para correlacionar picos de venta con picos de soporte
-- ============================================================

SELECT
    dt.anio,
    dt.mes,
    dt.nombre_mes,
    COALESCE(v.total_ventas, 0)     AS total_ventas,
    COALESCE(v.num_ordenes, 0)      AS ordenes_mes,
    COALESCE(s.num_llamadas, 0)     AS llamadas_servicio,
    ROUND(
        COALESCE(s.num_llamadas, 0)::NUMERIC /
        NULLIF(COALESCE(v.num_ordenes, 0), 0)
    , 2)                            AS ratio_llamadas_orden
FROM (
    SELECT DISTINCT anio, mes, nombre_mes
    FROM dw.dim_tiempo
) dt
LEFT JOIN (
    SELECT
        dt2.anio,
        dt2.mes,
        ROUND(SUM(fv.monto_linea)::NUMERIC, 2)       AS total_ventas,
        COUNT(DISTINCT fv.order_number)               AS num_ordenes
    FROM dw.fact_ventas fv
    JOIN dw.dim_tiempo dt2 ON dt2.tiempo_key = fv.tiempo_key
    GROUP BY dt2.anio, dt2.mes
) v ON v.anio = dt.anio AND v.mes = dt.mes
LEFT JOIN (
    SELECT
        dt3.anio,
        dt3.mes,
        COUNT(*) AS num_llamadas
    FROM dw.fact_servicio fs
    JOIN dw.dim_tiempo dt3 ON dt3.tiempo_key = fs.tiempo_key
    GROUP BY dt3.anio, dt3.mes
) s ON s.anio = dt.anio AND s.mes = dt.mes
WHERE COALESCE(v.total_ventas, 0) > 0
   OR COALESCE(s.num_llamadas, 0) > 0
ORDER BY dt.anio, dt.mes;
