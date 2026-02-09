-- ==========================================================
-- 02_load_insert.sql  (Bronze)
-- ==========================================================
-- Proposito : Cargar los archivos CSV crudos en las tablas
--             bronze. Usa TRUNCATE + \copy (carga idempotente).
-- Dependencias: 01_ddl.sql (tablas bronze creadas)
-- Ejecucion :
--   Ejecutar desde la raiz del repositorio:
--   psql -d <base> -f sql/bronze/02_load_insert.sql
--
--   Las rutas son relativas al directorio de trabajo (CWD),
--   por eso es necesario ejecutar desde la raiz del repo
--   donde se encuentra la carpeta data/.
-- ==========================================================

\set ON_ERROR_STOP on

\echo '========================================'
\echo 'INICIO CARGA BRONZE - CAUDALES'
\echo '========================================'

-- ==========================================================
-- CALAMAR — CSV estructurado, encoding LATIN1
-- ==========================================================
\echo 'Cargando BRONZE - Calamar...'
TRUNCATE bronze.calamar;
\copy bronze.calamar FROM './data/raw/Calamar.csv' CSV HEADER DELIMITER ';' ENCODING 'LATIN1'
\echo 'OK - Calamar'

-- ==========================================================
-- CIUDAD BOLIVAR — Texto plano, una linea por registro
-- ==========================================================
\echo 'Cargando BRONZE - Ciudad Bolivar...'
TRUNCATE bronze.ciudad_bolivar;
\copy bronze.ciudad_bolivar(raw_line) FROM './data/raw/CiudadBolivar.csv' WITH (FORMAT text)
\echo 'OK - Ciudad Bolivar'

-- ==========================================================
-- MANAOS — Texto plano, una linea por registro
-- ==========================================================
\echo 'Cargando BRONZE - Manaos...'
TRUNCATE bronze.manaos;
\copy bronze.manaos(raw_line) FROM './data/raw/Manaos.csv' WITH (FORMAT text)
\echo 'OK - Manaos'

-- ==========================================================
-- OBIDOS — CSV con 5 columnas, encoding LATIN1
-- ==========================================================
\echo 'Cargando BRONZE - Obidos...'
TRUNCATE bronze.obidos;
\copy bronze.obidos FROM './data/raw/Obidos.csv' CSV HEADER DELIMITER ';' ENCODING 'LATIN1'
\echo 'OK - Obidos'

-- ==========================================================
-- TABATINGA — Texto plano, una linea por registro
-- ==========================================================
\echo 'Cargando BRONZE - Tabatinga...'
TRUNCATE bronze.tabatinga;
\copy bronze.tabatinga(raw_line) FROM './data/raw/Tabatinga.csv' WITH (FORMAT text)
\echo 'OK - Tabatinga'

-- ==========================================================
-- TIMBUES — Texto plano, una linea por registro
-- ==========================================================
\echo 'Cargando BRONZE - Timbues...'
TRUNCATE bronze.timbues;
\copy bronze.timbues(raw_line) FROM './data/raw/Timbues.csv' WITH (FORMAT text)
\echo 'OK - Timbues'

\echo '========================================'
\echo 'CARGA BRONZE FINALIZADA CORRECTAMENTE'
\echo '========================================'
