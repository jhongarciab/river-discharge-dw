-- Calamar
INSERT INTO silver.flow_monthly (
  year,
  month,
  calamar_monthly
)
WITH mensual_real AS (
  SELECT
    EXTRACT(YEAR  FROM fecha::date)::int  AS year,
    EXTRACT(MONTH FROM fecha::date)::int AS month,
    AVG(valor::double precision)         AS calamar_monthly
  FROM bronze.calamar
  WHERE
    regexp_replace(nombre_estacion, '\s*\[.*\]', '') = 'CALAMAR'
    AND descripcion_serie = 'Caudal medio diario'
    AND fecha::date < DATE '2017-01-01'
    AND valor IS NOT NULL
    AND valor ~ '^[0-9]+(\.[0-9]+)?$'
    AND valor::double precision <= 16000
  GROUP BY
    EXTRACT(YEAR  FROM fecha::date),
    EXTRACT(MONTH FROM fecha::date)
),
climatologia_mensual AS (
  SELECT
    month,
    AVG(calamar_monthly) AS calamar_monthly
  FROM mensual_real
  GROUP BY month
),
meses_faltantes (year, month) AS (
  VALUES
    (1956,12),
    (1970,11),
    (1970,12),
    (2016,1),
    (2016,10)
),
mensual_imputado AS (
  SELECT
    f.year,
    f.month,
    c.calamar_monthly
  FROM meses_faltantes f
  JOIN climatologia_mensual c
    ON c.month = f.month
),
serie_final AS (
  SELECT year, month, calamar_monthly FROM mensual_real
  UNION ALL
  SELECT year, month, calamar_monthly FROM mensual_imputado
)
SELECT
  year,
  month,
  calamar_monthly
FROM serie_final
ORDER BY year, month;

-- Ciudad Bolivar
INSERT INTO silver.flow_monthly (
  year,
  month,
  bolivar_monthly
)
WITH diario_limpio AS (
  SELECT
    to_timestamp(
      split_part(raw_line, ';', 3),
      'DD/MM/YYYY HH24:MI'
    )                                                    AS fecha_ts,
    REPLACE(
      NULLIF(split_part(raw_line, ';', 4), ''),
      ',', '.'
    )::double precision                                  AS value
  FROM bronze.ciudad_bolivar
  WHERE
    raw_line ~ '^[0-9]'
),
mensual_real AS (
  SELECT
    EXTRACT(YEAR  FROM fecha_ts)::int   AS year,
    EXTRACT(MONTH FROM fecha_ts)::int  AS month,
    AVG(value)                         AS bolivar_monthly
  FROM diario_limpio
  WHERE value IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  year,
  month,
  bolivar_monthly
FROM mensual_real
ON CONFLICT (year, month)
DO UPDATE SET bolivar_monthly = EXCLUDED.bolivar_monthly;

-- Manaos
INSERT INTO silver.flow_monthly (
  year,
  month,
  manaos_monthly
)
WITH diario_limpio AS (
  SELECT
    to_timestamp(
      split_part(raw_line, ';', 3),
      'DD/MM/YYYY HH24:MI'
    )                                                     AS fecha_ts,
    REPLACE(
      NULLIF(split_part(raw_line, ';', 4), ''),
      ',', '.'
    )::double precision                                   AS value
  FROM bronze.manaos
  WHERE
    raw_line ~ '^[0-9]'
),
mensual_real AS (
  -- Promedios mensuales observados
  SELECT
    EXTRACT(YEAR  FROM fecha_ts)::int    AS year,
    EXTRACT(MONTH FROM fecha_ts)::int   AS month,
    AVG(value)                          AS manaos_monthly
  FROM diario_limpio
  WHERE value IS NOT NULL
  GROUP BY 1, 2
),
climatologia_mensual AS (
  -- Promedio histórico por mes
  SELECT
    month,
    AVG(manaos_monthly) AS manaos_monthly
  FROM mensual_real
  GROUP BY month
),
mensual_final AS (
  -- Meses reales + meses imputados
  SELECT
    m.year,
    m.month,
    m.manaos_monthly
  FROM mensual_real m

  UNION ALL

  SELECT
    c.year,
    c.month,
    cl.manaos_monthly
  FROM (
    SELECT DISTINCT
      EXTRACT(YEAR  FROM fecha_ts)::int  AS year,
      EXTRACT(MONTH FROM fecha_ts)::int AS month
    FROM diario_limpio
  ) c
  LEFT JOIN mensual_real m
    ON m.year = c.year AND m.month = c.month
  JOIN climatologia_mensual cl
    ON cl.month = c.month
  WHERE m.year IS NULL
)
SELECT
  year,
  month,
  manaos_monthly
FROM mensual_final
ON CONFLICT (year, month)
DO UPDATE SET manaos_monthly = EXCLUDED.manaos_monthly;

WITH climatologia AS (
  SELECT
    month,
    AVG(manaos_monthly) AS clim_value
  FROM silver.flow_monthly
  WHERE manaos_monthly IS NOT NULL
  GROUP BY month
)
UPDATE silver.flow_monthly s
SET manaos_monthly = c.clim_value
FROM climatologia c
WHERE
  s.manaos_monthly IS NULL
  AND (s.year, s.month) IN (
    (2003,11),
    (2003,12),
    (2004,1),
    (2016,10),
    (2016,11),
    (2016,12),
    (2017,1),
    (2017,2),
    (2017,3)
  )
  AND s.month = c.month;

-- Obidos
INSERT INTO silver.flow_monthly (
  year,
  month,
  obidos_monthly
)
WITH diario_limpio AS (
  SELECT
    to_timestamp(fecha, 'DD/MM/YYYY HH24:MI')              AS fecha_ts,
    REPLACE(NULLIF(valor, ''), ',', '.')::double precision AS value
  FROM bronze.obidos
  WHERE
    valor IS NOT NULL
),
mensual_real AS (
  SELECT
    EXTRACT(YEAR  FROM fecha_ts)::int   AS year,
    EXTRACT(MONTH FROM fecha_ts)::int  AS month,
    AVG(value)                         AS obidos_monthly
  FROM diario_limpio
  WHERE value IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  year,
  month,
  obidos_monthly
FROM mensual_real
ON CONFLICT (year, month)
DO UPDATE SET obidos_monthly = EXCLUDED.obidos_monthly;

WITH climatologia AS (
  SELECT
    AVG(obidos_monthly) AS clim_value
  FROM silver.flow_monthly
  WHERE
    obidos_monthly IS NOT NULL
    AND month = 12
)
UPDATE silver.flow_monthly
SET obidos_monthly = (
  SELECT clim_value FROM climatologia
)
WHERE
  year = 2016
  AND month = 12
  AND obidos_monthly IS NULL;

-- Tabatinga
INSERT INTO silver.flow_monthly (
  year,
  month,
  tabatinga_monthly
)
WITH diario_limpio AS (
  SELECT
    to_timestamp(
      split_part(raw_line, ';', 3),
      'DD/MM/YYYY HH24:MI'
    )                                                     AS fecha_ts,
    REPLACE(
      NULLIF(split_part(raw_line, ';', 4), ''),
      ',', '.'
    )::double precision                                   AS value
  FROM bronze.tabatinga
  WHERE
    raw_line ~ '^[0-9]'          -- elimina headers, filas vacías y basura
),
mensual_real AS (
  SELECT
    EXTRACT(YEAR  FROM fecha_ts)::int   AS year,
    EXTRACT(MONTH FROM fecha_ts)::int  AS month,
    AVG(value)                         AS tabatinga_monthly
  FROM diario_limpio
  WHERE value IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  year,
  month,
  tabatinga_monthly
FROM mensual_real
ON CONFLICT (year, month)
DO UPDATE SET tabatinga_monthly = EXCLUDED.tabatinga_monthly;

-- Timbues
INSERT INTO silver.flow_monthly (
  year,
  month,
  timbues_monthly
)
SELECT
  EXTRACT(YEAR  FROM to_date(split_part(raw_line, ';', 1), 'DD/MM/YYYY'))::int  AS year,
  EXTRACT(MONTH FROM to_date(split_part(raw_line, ';', 1), 'DD/MM/YYYY'))::int AS month,
  REPLACE(
    NULLIF(split_part(raw_line, ';', 2), ''),
    ',', '.'
  )::double precision                                                          AS timbues_monthly
FROM bronze.timbues
WHERE
  raw_line ~ '^[0-9]'      -- elimina basura / headers
ON CONFLICT (year, month)
DO UPDATE SET timbues_monthly = EXCLUDED.timbues_monthly;
