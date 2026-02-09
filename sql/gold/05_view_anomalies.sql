-- ==========================================================
-- 05_view_anomalies.sql  (Gold)
-- ==========================================================
-- Proposito : Vista de anomalias estandarizadas (z-scores)
--             de caudal mensual por estacion.
--
--             z = (valor_observado - media_mensual) / desv_std_mensual
--
--             La media y desviacion estandar se calculan por
--             estacion y mes del anio (climatologia mensual),
--             usando todos los anios disponibles en Silver.
--
--             Al ser una VIEW (no tabla), las anomalias se
--             recalculan automaticamente cuando Silver cambia.
--
-- Dependencias: silver.flow_monthly (tabla con datos limpios)
-- Ejecucion :
--   psql -d <base> -f sql/gold/05_view_anomalies.sql
-- Consulta :
--   SELECT * FROM gold.flow_monthly_anomalies
--   WHERE year = 2015 ORDER BY month;
-- ==========================================================

\set ON_ERROR_STOP on

-- Eliminar tabla legacy si existe (migracion de tabla a VIEW)
DROP TABLE IF EXISTS gold.flow_monthly_anomalies;
DROP VIEW  IF EXISTS gold.flow_monthly_anomalies;

CREATE OR REPLACE VIEW gold.flow_monthly_anomalies AS

-- 1. Normalizar a formato largo: una fila por (year, month, estacion)
WITH base AS (
    SELECT year, month, 'calamar'  AS station, calamar_monthly   AS value FROM silver.flow_monthly WHERE calamar_monthly   IS NOT NULL
    UNION ALL
    SELECT year, month, 'bolivar',              bolivar_monthly          FROM silver.flow_monthly WHERE bolivar_monthly   IS NOT NULL
    UNION ALL
    SELECT year, month, 'manaos',               manaos_monthly           FROM silver.flow_monthly WHERE manaos_monthly    IS NOT NULL
    UNION ALL
    SELECT year, month, 'obidos',               obidos_monthly           FROM silver.flow_monthly WHERE obidos_monthly    IS NOT NULL
    UNION ALL
    SELECT year, month, 'tabatinga',            tabatinga_monthly        FROM silver.flow_monthly WHERE tabatinga_monthly IS NOT NULL
    UNION ALL
    SELECT year, month, 'timbues',              timbues_monthly          FROM silver.flow_monthly WHERE timbues_monthly   IS NOT NULL
),

-- 2. Estadisticos climatologicos por estacion y mes del anio
stats AS (
    SELECT
        station,
        month,
        AVG(value)         AS mean_month,
        STDDEV_SAMP(value) AS std_month   -- desviacion estandar muestral (n-1)
    FROM base
    GROUP BY station, month
),

-- 3. Calcular z-score por observacion
--    NULLIF protege contra division por cero cuando solo hay 1 anio de datos
anom AS (
    SELECT
        b.year,
        b.month,
        b.station,
        (b.value - s.mean_month) / NULLIF(s.std_month, 0) AS anomaly
    FROM base b
    JOIN stats s
      ON b.station = s.station
     AND b.month   = s.month
)

-- 4. Pivotar de vuelta a formato wide (una columna por estacion)
SELECT
    year,
    month,
    MAX(anomaly) FILTER (WHERE station = 'calamar')   AS calamar_anomaly,
    MAX(anomaly) FILTER (WHERE station = 'bolivar')   AS bolivar_anomaly,
    MAX(anomaly) FILTER (WHERE station = 'manaos')    AS manaos_anomaly,
    MAX(anomaly) FILTER (WHERE station = 'obidos')    AS obidos_anomaly,
    MAX(anomaly) FILTER (WHERE station = 'tabatinga') AS tabatinga_anomaly,
    MAX(anomaly) FILTER (WHERE station = 'timbues')   AS timbues_anomaly
FROM anom
GROUP BY year, month
ORDER BY year, month;

COMMENT ON VIEW gold.flow_monthly_anomalies IS
    'Anomalias estandarizadas (z-scores) de caudal mensual por estacion. Calculadas respecto a la climatologia mensual de silver.flow_monthly.';
