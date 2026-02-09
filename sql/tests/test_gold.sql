-- ==========================================================
-- test_gold.sql
-- ==========================================================
-- Proposito : Validar la vista gold.flow_monthly_anomalies
--             (z-scores de caudal mensual por estacion).
-- Dependencias: Silver debe estar poblado y la VIEW creada.
-- Ejecucion :
--   psql -d <base> -f sql/tests/test_gold.sql
--
--   Si algun test falla, el script se detiene con un error
--   EXCEPTION. Si todos pasan, imprime NOTICE con 'PASS'.
-- ==========================================================

\set ON_ERROR_STOP on

\echo '========================================'
\echo 'TESTS - CAPA GOLD'
\echo '========================================'


-- ----------------------------------------------------------
-- TEST 1: La VIEW no esta vacia
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n FROM gold.flow_monthly_anomalies;
    IF n = 0 THEN
        RAISE EXCEPTION 'FAIL: gold.flow_monthly_anomalies esta vacia';
    END IF;
    RAISE NOTICE 'PASS: gold.flow_monthly_anomalies tiene % filas', n;
END $$;


-- ----------------------------------------------------------
-- TEST 2: La cantidad de filas coincide con Silver
-- Cada fila de Silver que tenga al menos un caudal no-NULL
-- debe generar una fila en Gold.
-- ----------------------------------------------------------
DO $$
DECLARE
    n_silver INT;
    n_gold   INT;
BEGIN
    SELECT COUNT(*) INTO n_silver
    FROM silver.flow_monthly
    WHERE calamar_monthly   IS NOT NULL
       OR bolivar_monthly   IS NOT NULL
       OR manaos_monthly    IS NOT NULL
       OR obidos_monthly    IS NOT NULL
       OR tabatinga_monthly IS NOT NULL
       OR timbues_monthly   IS NOT NULL;

    SELECT COUNT(*) INTO n_gold FROM gold.flow_monthly_anomalies;

    IF n_silver <> n_gold THEN
        RAISE EXCEPTION 'FAIL: Silver tiene % filas con datos, Gold tiene %', n_silver, n_gold;
    END IF;
    RAISE NOTICE 'PASS: Gold (%) coincide con Silver (%) en numero de filas', n_gold, n_silver;
END $$;


-- ----------------------------------------------------------
-- TEST 3: La media de anomalias por estacion-mes es aprox. 0
-- Por definicion de z-score, la media debe ser 0.
-- Tolerancia de 0.001 para errores de punto flotante.
-- ----------------------------------------------------------
DO $$
DECLARE
    max_mean DOUBLE PRECISION;
BEGIN
    SELECT MAX(ABS(mean_anom)) INTO max_mean
    FROM (
        SELECT AVG(calamar_anomaly)   AS mean_anom FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(bolivar_anomaly)   FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(manaos_anomaly)    FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(obidos_anomaly)    FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(tabatinga_anomaly) FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(timbues_anomaly)   FROM gold.flow_monthly_anomalies GROUP BY month
    ) sub;

    IF max_mean > 0.001 THEN
        RAISE EXCEPTION 'FAIL: Media maxima de anomalias por mes = % (esperado ~0)', max_mean;
    END IF;
    RAISE NOTICE 'PASS: Media de anomalias por estacion-mes es ~0 (max: %)', ROUND(max_mean::numeric, 6);
END $$;


-- ----------------------------------------------------------
-- TEST 4: No hay anomalias extremas (|z| > 5)
-- Un z-score > 5 es estadisticamente muy improbable y podria
-- indicar un error en los datos fuente o en la limpieza.
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM gold.flow_monthly_anomalies
    WHERE ABS(calamar_anomaly)   > 5
       OR ABS(bolivar_anomaly)   > 5
       OR ABS(manaos_anomaly)    > 5
       OR ABS(obidos_anomaly)    > 5
       OR ABS(tabatinga_anomaly) > 5
       OR ABS(timbues_anomaly)   > 5;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con anomalia |z| > 5 (revisar datos fuente)', n;
    END IF;
    RAISE NOTICE 'PASS: Todas las anomalias tienen |z| <= 5';
END $$;


-- ----------------------------------------------------------
-- TEST 5: Anomalias NULL solo donde Silver tiene NULL
-- Si una estacion tiene dato en Silver, debe tener anomalia.
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_monthly s
    JOIN gold.flow_monthly_anomalies g
      ON s.year = g.year AND s.month = g.month
    WHERE (s.calamar_monthly   IS NOT NULL AND g.calamar_anomaly   IS NULL)
       OR (s.bolivar_monthly   IS NOT NULL AND g.bolivar_anomaly   IS NULL)
       OR (s.manaos_monthly    IS NOT NULL AND g.manaos_anomaly    IS NULL)
       OR (s.tabatinga_monthly IS NOT NULL AND g.tabatinga_anomaly IS NULL)
       OR (s.obidos_monthly    IS NOT NULL AND g.obidos_anomaly    IS NULL)
       OR (s.timbues_monthly   IS NOT NULL AND g.timbues_anomaly   IS NULL);

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con dato en Silver pero sin anomalia en Gold', n;
    END IF;
    RAISE NOTICE 'PASS: Toda estacion con dato en Silver tiene anomalia en Gold';
END $$;


-- ----------------------------------------------------------
-- TEST 6: Desviacion estandar de anomalias es aprox. 1
-- Por definicion de z-score, la desv. std. debe ser ~1.
-- Tolerancia de 0.05 por muestras finitas.
-- ----------------------------------------------------------
DO $$
DECLARE
    max_diff DOUBLE PRECISION;
BEGIN
    SELECT MAX(ABS(std_anom - 1.0)) INTO max_diff
    FROM (
        SELECT STDDEV_SAMP(calamar_anomaly)   AS std_anom FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT STDDEV_SAMP(bolivar_anomaly)   FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT STDDEV_SAMP(manaos_anomaly)    FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT STDDEV_SAMP(obidos_anomaly)    FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT STDDEV_SAMP(tabatinga_anomaly) FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT STDDEV_SAMP(timbues_anomaly)   FROM gold.flow_monthly_anomalies GROUP BY month
    ) sub
    WHERE std_anom IS NOT NULL;

    IF max_diff > 0.05 THEN
        RAISE EXCEPTION 'FAIL: Desv. std. de anomalias difiere de 1.0 en % (max tolerado: 0.05)', ROUND(max_diff::numeric, 4);
    END IF;
    RAISE NOTICE 'PASS: Desv. std. de anomalias por estacion-mes es ~1 (max diff: %)', ROUND(max_diff::numeric, 6);
END $$;


\echo '========================================'
\echo 'TODOS LOS TESTS GOLD PASARON'
\echo '========================================'
