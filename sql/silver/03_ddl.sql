-- ==========================================================
-- 03_ddl.sql  (Silver)
-- ==========================================================
-- Proposito : Crear la tabla de caudales mensuales limpios
--             y armonizados. Consolida las 6 estaciones en
--             formato wide (una columna por estacion).
-- Dependencias: 00_init_schemas.sql
-- Ejecucion :
--   psql -d <base> -f sql/silver/03_ddl.sql
-- Notas:
--   - Valores en m3/s (metros cubicos por segundo).
--   - NULL indica que no hay dato para esa estacion-mes.
--   - Los valores pueden ser observados o imputados por
--     climatologia (ver 04_insert.sql para detalle).
-- ==========================================================

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS silver.flow_monthly;

CREATE TABLE silver.flow_monthly (
    year  INT NOT NULL,
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),

    calamar_monthly    DOUBLE PRECISION,  -- Magdalena en Calamar (Colombia)
    bolivar_monthly    DOUBLE PRECISION,  -- Orinoco en Ciudad Bolivar (Venezuela)
    manaos_monthly     DOUBLE PRECISION,  -- Negro/Amazonas en Manaos (Brasil)
    obidos_monthly     DOUBLE PRECISION,  -- Amazonas en Obidos (Brasil)
    tabatinga_monthly  DOUBLE PRECISION,  -- Amazonas en Tabatinga (Brasil)
    timbues_monthly    DOUBLE PRECISION,  -- Parana en Timbues (Argentina)

    CONSTRAINT pk_flow_monthly PRIMARY KEY (year, month)
);

-- ---- Metadatos de la tabla ----
COMMENT ON TABLE silver.flow_monthly IS
    'Caudales mensuales promedio (m3/s) por estacion, limpios y con imputacion climatologica.';

COMMENT ON COLUMN silver.flow_monthly.year              IS 'Anio de la observacion';
COMMENT ON COLUMN silver.flow_monthly.month             IS 'Mes de la observacion (1-12)';
COMMENT ON COLUMN silver.flow_monthly.calamar_monthly   IS 'Caudal promedio mensual en Calamar - Rio Magdalena (m3/s)';
COMMENT ON COLUMN silver.flow_monthly.bolivar_monthly   IS 'Caudal promedio mensual en Ciudad Bolivar - Rio Orinoco (m3/s)';
COMMENT ON COLUMN silver.flow_monthly.manaos_monthly    IS 'Caudal promedio mensual en Manaos - Rio Negro/Amazonas (m3/s)';
COMMENT ON COLUMN silver.flow_monthly.obidos_monthly    IS 'Caudal promedio mensual en Obidos - Rio Amazonas (m3/s)';
COMMENT ON COLUMN silver.flow_monthly.tabatinga_monthly IS 'Caudal promedio mensual en Tabatinga - Rio Amazonas (m3/s)';
COMMENT ON COLUMN silver.flow_monthly.timbues_monthly   IS 'Caudal promedio mensual en Timbues - Rio Parana (m3/s)';
