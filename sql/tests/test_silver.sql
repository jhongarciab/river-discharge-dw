-- ==========================================================
-- test_silver.sql
-- ==========================================================
-- Proposito : Validar la integridad y calidad de los datos
--             en silver.flow_monthly despues de ejecutar el
--             pipeline Bronze -> Silver.
-- Ejecucion :
--   psql -d <base> -f sql/tests/test_silver.sql
--
--   Si algun test falla, el script se detiene con un error
--   EXCEPTION. Si todos pasan, imprime NOTICE con 'PASS'.
-- ==========================================================

\set ON_ERROR_STOP on

\echo '========================================'
\echo 'TESTS - CAPA SILVER'
\echo '========================================'


-- ----------------------------------------------------------
-- TEST 1: La tabla no esta vacia
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n FROM silver.flow_monthly;
    IF n = 0 THEN
        RAISE EXCEPTION 'FAIL: silver.flow_monthly esta vacia (0 filas)';
    END IF;
    RAISE NOTICE 'PASS: silver.flow_monthly tiene % filas', n;
END $$;


-- ----------------------------------------------------------
-- TEST 2: No hay duplicados en la clave primaria (year, month)
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM (
        SELECT year, month
        FROM silver.flow_monthly
        GROUP BY year, month
        HAVING COUNT(*) > 1
    ) dups;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % combinaciones (year, month) duplicadas', n;
    END IF;
    RAISE NOTICE 'PASS: No hay duplicados en (year, month)';
END $$;


-- ----------------------------------------------------------
-- TEST 3: Todos los meses estan entre 1 y 12
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_monthly
    WHERE month < 1 OR month > 12;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con mes fuera de rango [1,12]', n;
    END IF;
    RAISE NOTICE 'PASS: Todos los meses estan entre 1 y 12';
END $$;


-- ----------------------------------------------------------
-- TEST 4: No hay valores negativos de caudal
-- Un caudal negativo es fisicamente imposible.
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_monthly
    WHERE calamar_monthly   < 0
       OR bolivar_monthly   < 0
       OR manaos_monthly    < 0
       OR obidos_monthly    < 0
       OR tabatinga_monthly < 0
       OR timbues_monthly   < 0;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con caudal negativo', n;
    END IF;
    RAISE NOTICE 'PASS: No hay valores negativos de caudal';
END $$;


-- ----------------------------------------------------------
-- TEST 5: Calamar no excede el umbral fisico (16000 m3/s)
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_monthly
    WHERE calamar_monthly > 16000;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con Calamar > 16000 m3/s', n;
    END IF;
    RAISE NOTICE 'PASS: Calamar dentro del umbral fisico (<=16000 m3/s)';
END $$;


-- ----------------------------------------------------------
-- TEST 6: Cada estacion tiene un minimo de registros
-- Umbrales minimos basados en los rangos temporales conocidos.
-- ----------------------------------------------------------
DO $$
DECLARE
    n_calamar   INT;
    n_bolivar   INT;
    n_manaos    INT;
    n_obidos    INT;
    n_tabatinga INT;
    n_timbues   INT;
BEGIN
    SELECT
        COUNT(calamar_monthly),
        COUNT(bolivar_monthly),
        COUNT(manaos_monthly),
        COUNT(obidos_monthly),
        COUNT(tabatinga_monthly),
        COUNT(timbues_monthly)
    INTO n_calamar, n_bolivar, n_manaos, n_obidos, n_tabatinga, n_timbues
    FROM silver.flow_monthly;

    -- Umbrales minimos razonables por estacion
    IF n_calamar < 500 THEN
        RAISE EXCEPTION 'FAIL: Calamar tiene solo % registros (esperado >=500)', n_calamar;
    END IF;
    IF n_bolivar < 100 THEN
        RAISE EXCEPTION 'FAIL: Bolivar tiene solo % registros (esperado >=100)', n_bolivar;
    END IF;
    IF n_manaos < 100 THEN
        RAISE EXCEPTION 'FAIL: Manaos tiene solo % registros (esperado >=100)', n_manaos;
    END IF;
    IF n_obidos < 100 THEN
        RAISE EXCEPTION 'FAIL: Obidos tiene solo % registros (esperado >=100)', n_obidos;
    END IF;
    IF n_tabatinga < 100 THEN
        RAISE EXCEPTION 'FAIL: Tabatinga tiene solo % registros (esperado >=100)', n_tabatinga;
    END IF;
    IF n_timbues < 50 THEN
        RAISE EXCEPTION 'FAIL: Timbues tiene solo % registros (esperado >=50)', n_timbues;
    END IF;

    RAISE NOTICE 'PASS: Conteos por estacion — Calamar:%, Bolivar:%, Manaos:%, Obidos:%, Tabatinga:%, Timbues:%',
        n_calamar, n_bolivar, n_manaos, n_obidos, n_tabatinga, n_timbues;
END $$;


-- ----------------------------------------------------------
-- TEST 7: Rangos fisicos razonables por estacion
-- Los umbrales superiores son aproximaciones del caudal
-- historico maximo conocido por estacion.
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_monthly
    WHERE calamar_monthly   > 16000   -- Magdalena en Calamar
       OR bolivar_monthly   > 100000  -- Orinoco en Ciudad Bolivar
       OR manaos_monthly    > 200000  -- Negro/Amazonas en Manaos
       OR obidos_monthly    > 300000  -- Amazonas en Obidos
       OR tabatinga_monthly > 80000   -- Amazonas en Tabatinga
       OR timbues_monthly   > 30000;  -- Parana en Timbues

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con caudal fuera de rango fisico', n;
    END IF;
    RAISE NOTICE 'PASS: Todos los caudales dentro de rangos fisicos razonables';
END $$;


-- ----------------------------------------------------------
-- TEST 8: Anios dentro de un rango historico razonable
-- ----------------------------------------------------------
DO $$
DECLARE
    min_year INT;
    max_year INT;
BEGIN
    SELECT MIN(year), MAX(year) INTO min_year, max_year
    FROM silver.flow_monthly;

    IF min_year < 1900 THEN
        RAISE EXCEPTION 'FAIL: Anio minimo % es anterior a 1900', min_year;
    END IF;
    IF max_year > 2030 THEN
        RAISE EXCEPTION 'FAIL: Anio maximo % es posterior a 2030', max_year;
    END IF;
    RAISE NOTICE 'PASS: Rango temporal [%, %] es razonable', min_year, max_year;
END $$;


\echo '========================================'
\echo 'TODOS LOS TESTS SILVER PASARON'
\echo '========================================'
