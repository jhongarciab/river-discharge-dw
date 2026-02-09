-- ==========================================================
-- 04_insert.sql  (Silver)
-- ==========================================================
-- Proposito : Transformar datos crudos de bronze a caudales
--             mensuales limpios en silver.flow_monthly.
--
--             Cada estacion sigue el mismo patron:
--               1. parsing    — extraer campos del CSV crudo
--               2. limpieza   — filtrar registros invalidos
--               3. mensual    — agregar diario a mensual (AVG)
--               4. climatologia — promedio historico por mes
--               5. imputacion — rellenar huecos con climatologia
--               6. serie_final — union de observados + imputados
--
-- Dependencias: 03_ddl.sql, 02_load_insert.sql (bronze cargado)
-- Ejecucion :
--   psql -d <base> -f sql/silver/04_insert.sql
-- ==========================================================

\set ON_ERROR_STOP on

\echo '========================================'
\echo 'INICIO TRANSFORMACION SILVER'
\echo '========================================'


-- ==========================================================
-- CALAMAR  (Rio Magdalena, Colombia)
-- ==========================================================
-- Fuente: IDEAM. CSV estructurado con metadatos por fila.
-- Filtros de calidad:
--   - Solo estacion 'CALAMAR' (sin sufijos entre corchetes)
--   - Solo serie 'Caudal medio diario'
--   - Datos hasta 2016-12 (a partir de 2017 hay cambio de sensor)
--   - Valores numericos positivos <= 16000 m3/s (umbral fisico:
--     el caudal historico maximo registrado en Calamar es ~15800)
-- Imputacion:
--   Meses faltantes conocidos se rellenan con climatologia mensual.
-- ==========================================================
\echo 'Procesando SILVER - Calamar...'

INSERT INTO silver.flow_monthly (year, month, calamar_monthly)
WITH parsing AS (
    SELECT
        fecha::date                    AS fecha,
        nombre_estacion,
        descripcion_serie,
        valor
    FROM bronze.calamar
),
limpieza AS (
    SELECT
        fecha,
        valor::double precision AS valor_num
    FROM parsing
    WHERE
        -- Filtrar solo la estacion CALAMAR (puede venir con sufijo [N])
        regexp_replace(nombre_estacion, '\s*\[.*\]', '') = 'CALAMAR'
        -- Solo la serie de caudal medio diario
        AND descripcion_serie = 'Caudal medio diario'
        -- Descartar datos post-2016 por cambio de sensor
        AND fecha < DATE '2017-01-01'
        -- El valor debe ser numerico valido
        AND valor IS NOT NULL
        AND valor ~ '^[0-9]+(\.[0-9]+)?$'
        -- Umbral fisico: caudal maximo historico ~15800 m3/s
        AND valor::double precision <= 16000
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha)::int AS year,
        EXTRACT(MONTH FROM fecha)::int AS month,
        AVG(valor_num)                 AS calamar_monthly
    FROM limpieza
    GROUP BY 1, 2
),
climatologia AS (
    SELECT month, AVG(calamar_monthly) AS valor_clima
    FROM mensual
    GROUP BY month
),
-- Meses con datos faltantes identificados en exploracion previa.
-- Se imputan con la climatologia mensual correspondiente.
meses_faltantes (year, month) AS (
    VALUES (1956, 12), (1970, 11), (1970, 12), (2016, 1), (2016, 10)
),
imputacion AS (
    SELECT f.year, f.month, c.valor_clima AS calamar_monthly
    FROM meses_faltantes f
    JOIN climatologia c ON c.month = f.month
),
serie_final AS (
    SELECT year, month, calamar_monthly FROM mensual
    UNION ALL
    SELECT year, month, calamar_monthly FROM imputacion
)
SELECT year, month, calamar_monthly
FROM serie_final
ORDER BY year, month;

\echo 'OK - Calamar'


-- ==========================================================
-- CIUDAD BOLIVAR  (Rio Orinoco, Venezuela)
-- ==========================================================
-- Fuente: GRDC / BNO. CSV semi-estructurado (raw_line).
-- Estructura del raw_line (separador ';'):
--   campo 1: id_estacion
--   campo 2: nombre
--   campo 3: fecha (DD/MM/YYYY HH24:MI)
--   campo 4: valor (usa coma como separador decimal)
--   campo 5: origen
-- Filtros de calidad:
--   - Solo lineas que comienzan con digito (excluir headers)
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL
-- Imputacion: No se requiere (serie completa).
-- ==========================================================
\echo 'Procesando SILVER - Ciudad Bolivar...'

INSERT INTO silver.flow_monthly (year, month, bolivar_monthly)
WITH parsing AS (
    SELECT
        to_timestamp(
            split_part(raw_line, ';', 3),
            'DD/MM/YYYY HH24:MI'
        ) AS fecha_ts,
        REPLACE(
            NULLIF(split_part(raw_line, ';', 4), ''),
            ',', '.'
        )::double precision AS valor_num
    FROM bronze.ciudad_bolivar
    WHERE raw_line ~ '^[0-9]'  -- descartar headers y lineas vacias
),
limpieza AS (
    SELECT fecha_ts, valor_num
    FROM parsing
    WHERE valor_num IS NOT NULL
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha_ts)::int AS year,
        EXTRACT(MONTH FROM fecha_ts)::int AS month,
        AVG(valor_num)                    AS bolivar_monthly
    FROM limpieza
    GROUP BY 1, 2
)
SELECT year, month, bolivar_monthly
FROM mensual
ON CONFLICT (year, month)
DO UPDATE SET bolivar_monthly = EXCLUDED.bolivar_monthly;

\echo 'OK - Ciudad Bolivar'


-- ==========================================================
-- MANAOS  (Rio Negro / Amazonas, Brasil)
-- ==========================================================
-- Fuente: ANA Brasil / GRDC. CSV semi-estructurado (raw_line).
-- Misma estructura de campos que Ciudad Bolivar.
-- Filtros de calidad:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL
-- Imputacion:
--   - Huecos automaticos: meses presentes en la serie temporal
--     del archivo pero sin datos observados se rellenan con
--     climatologia mensual.
--   - Huecos adicionales fuera del rango del archivo (meses
--     donde la serie no existia pero que caen dentro del rango
--     global de silver): se imputan individualmente con
--     climatologia.
-- ==========================================================
\echo 'Procesando SILVER - Manaos...'

INSERT INTO silver.flow_monthly (year, month, manaos_monthly)
WITH parsing AS (
    SELECT
        to_timestamp(
            split_part(raw_line, ';', 3),
            'DD/MM/YYYY HH24:MI'
        ) AS fecha_ts,
        REPLACE(
            NULLIF(split_part(raw_line, ';', 4), ''),
            ',', '.'
        )::double precision AS valor_num
    FROM bronze.manaos
    WHERE raw_line ~ '^[0-9]'
),
limpieza AS (
    SELECT fecha_ts, valor_num
    FROM parsing
    WHERE valor_num IS NOT NULL
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha_ts)::int AS year,
        EXTRACT(MONTH FROM fecha_ts)::int AS month,
        AVG(valor_num)                    AS manaos_monthly
    FROM limpieza
    GROUP BY 1, 2
),
climatologia AS (
    SELECT month, AVG(manaos_monthly) AS valor_clima
    FROM mensual
    GROUP BY month
),
-- Detectar meses dentro del rango temporal del archivo que
-- no tienen observacion y rellenarlos con climatologia.
meses_en_rango AS (
    SELECT DISTINCT
        EXTRACT(YEAR  FROM fecha_ts)::int AS year,
        EXTRACT(MONTH FROM fecha_ts)::int AS month
    FROM parsing
),
huecos_automaticos AS (
    SELECT r.year, r.month, c.valor_clima AS manaos_monthly
    FROM meses_en_rango r
    LEFT JOIN mensual m ON m.year = r.year AND m.month = r.month
    JOIN climatologia c  ON c.month = r.month
    WHERE m.year IS NULL  -- no hay observacion para este mes
),
-- Huecos adicionales fuera del rango del archivo, identificados
-- manualmente durante la exploracion de datos.
meses_faltantes_extra (year, month) AS (
    VALUES
        (2003, 11), (2003, 12),
        (2004,  1),
        (2016, 10), (2016, 11), (2016, 12),
        (2017,  1), (2017,  2), (2017,  3)
),
imputacion_extra AS (
    SELECT f.year, f.month, c.valor_clima AS manaos_monthly
    FROM meses_faltantes_extra f
    JOIN climatologia c ON c.month = f.month
    -- Evitar duplicar si ya fue cubierto por huecos_automaticos
    LEFT JOIN huecos_automaticos h ON h.year = f.year AND h.month = f.month
    WHERE h.year IS NULL
),
serie_final AS (
    SELECT year, month, manaos_monthly FROM mensual
    UNION ALL
    SELECT year, month, manaos_monthly FROM huecos_automaticos
    UNION ALL
    SELECT year, month, manaos_monthly FROM imputacion_extra
)
SELECT year, month, manaos_monthly
FROM serie_final
ON CONFLICT (year, month)
DO UPDATE SET manaos_monthly = EXCLUDED.manaos_monthly;

\echo 'OK - Manaos'


-- ==========================================================
-- OBIDOS  (Rio Amazonas, Brasil)
-- ==========================================================
-- Fuente: ANA Brasil / GRDC. CSV con 5 columnas.
-- Filtros de calidad:
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL
-- Imputacion:
--   Diciembre 2016 faltante, se rellena con climatologia de
--   diciembre.
-- ==========================================================
\echo 'Procesando SILVER - Obidos...'

INSERT INTO silver.flow_monthly (year, month, obidos_monthly)
WITH parsing AS (
    SELECT
        to_timestamp(fecha, 'DD/MM/YYYY HH24:MI') AS fecha_ts,
        REPLACE(NULLIF(valor, ''), ',', '.')::double precision AS valor_num
    FROM bronze.obidos
    WHERE valor IS NOT NULL
),
limpieza AS (
    SELECT fecha_ts, valor_num
    FROM parsing
    WHERE valor_num IS NOT NULL
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha_ts)::int AS year,
        EXTRACT(MONTH FROM fecha_ts)::int AS month,
        AVG(valor_num)                    AS obidos_monthly
    FROM limpieza
    GROUP BY 1, 2
),
climatologia AS (
    SELECT month, AVG(obidos_monthly) AS valor_clima
    FROM mensual
    GROUP BY month
),
-- Diciembre 2016 no tiene datos; se rellena con climatologia.
meses_faltantes (year, month) AS (
    VALUES (2016, 12)
),
imputacion AS (
    SELECT f.year, f.month, c.valor_clima AS obidos_monthly
    FROM meses_faltantes f
    JOIN climatologia c ON c.month = f.month
),
serie_final AS (
    SELECT year, month, obidos_monthly FROM mensual
    UNION ALL
    SELECT year, month, obidos_monthly FROM imputacion
)
SELECT year, month, obidos_monthly
FROM serie_final
ON CONFLICT (year, month)
DO UPDATE SET obidos_monthly = EXCLUDED.obidos_monthly;

\echo 'OK - Obidos'


-- ==========================================================
-- TABATINGA  (Rio Amazonas, Brasil)
-- ==========================================================
-- Fuente: ANA Brasil / GRDC. CSV semi-estructurado (raw_line).
-- Misma estructura de campos que Ciudad Bolivar y Manaos.
-- Filtros de calidad:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL
-- Imputacion: No se requiere (serie completa).
-- ==========================================================
\echo 'Procesando SILVER - Tabatinga...'

INSERT INTO silver.flow_monthly (year, month, tabatinga_monthly)
WITH parsing AS (
    SELECT
        to_timestamp(
            split_part(raw_line, ';', 3),
            'DD/MM/YYYY HH24:MI'
        ) AS fecha_ts,
        REPLACE(
            NULLIF(split_part(raw_line, ';', 4), ''),
            ',', '.'
        )::double precision AS valor_num
    FROM bronze.tabatinga
    WHERE raw_line ~ '^[0-9]'
),
limpieza AS (
    SELECT fecha_ts, valor_num
    FROM parsing
    WHERE valor_num IS NOT NULL
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha_ts)::int AS year,
        EXTRACT(MONTH FROM fecha_ts)::int AS month,
        AVG(valor_num)                    AS tabatinga_monthly
    FROM limpieza
    GROUP BY 1, 2
)
SELECT year, month, tabatinga_monthly
FROM mensual
ON CONFLICT (year, month)
DO UPDATE SET tabatinga_monthly = EXCLUDED.tabatinga_monthly;

\echo 'OK - Tabatinga'


-- ==========================================================
-- TIMBUES  (Rio Parana, Argentina)
-- ==========================================================
-- Fuente: INA Argentina. CSV semi-estructurado (raw_line).
-- Estructura del raw_line (separador ';'):
--   campo 1: fecha (DD/MM/YYYY)
--   campo 2: caudal (usa coma como separador decimal)
-- A diferencia de las otras estaciones, este CSV ya contiene
-- datos mensuales (no diarios), por lo que no se agrega.
-- Filtros de calidad:
--   - Solo lineas que comienzan con digito (excluir headers)
--   - Reemplazar coma decimal por punto
-- Imputacion: No se requiere (serie completa).
-- ==========================================================
\echo 'Procesando SILVER - Timbues...'

INSERT INTO silver.flow_monthly (year, month, timbues_monthly)
WITH parsing AS (
    SELECT
        to_date(split_part(raw_line, ';', 1), 'DD/MM/YYYY') AS fecha,
        REPLACE(
            NULLIF(split_part(raw_line, ';', 2), ''),
            ',', '.'
        )::double precision AS valor_num
    FROM bronze.timbues
    WHERE raw_line ~ '^[0-9]'
)
SELECT
    EXTRACT(YEAR  FROM fecha)::int AS year,
    EXTRACT(MONTH FROM fecha)::int AS month,
    valor_num                      AS timbues_monthly
FROM parsing
ON CONFLICT (year, month)
DO UPDATE SET timbues_monthly = EXCLUDED.timbues_monthly;

\echo 'OK - Timbues'


\echo '========================================'
\echo 'TRANSFORMACION SILVER FINALIZADA'
\echo '========================================'
