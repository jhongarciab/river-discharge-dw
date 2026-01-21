\set ON_ERROR_STOP on

\echo '========================================'
\echo 'INICIO CARGA BRONZE - CAUDALES'
\echo '========================================'

-- =================================================
-- CALAMAR
-- =================================================
\echo 'Cargando BRONZE - Calamar...'
TRUNCATE bronze.calamar;
\copy bronze.calamar FROM '/Users/jhongarciabarrera/VS - Local/Github/caudales-dw/data/raw/Calamar.csv' CSV HEADER DELIMITER ';' ENCODING 'LATIN1';
\echo 'OK - Calamar'

-- =================================================
-- CIUDAD BOLIVAR
-- =================================================
\echo 'Cargando BRONZE - Ciudad Bolivar...'
TRUNCATE bronze.ciudad_bolivar;
\copy bronze.ciudad_bolivar(raw_line) FROM '/Users/jhongarciabarrera/VS - Local/Github/caudales-dw/data/raw/CiudadBolivar.csv' WITH (FORMAT text);
\echo 'OK - Ciudad Bolivar'

-- =================================================
-- MANAOS
-- =================================================
\echo 'Cargando BRONZE - Manaos...'
TRUNCATE bronze.manaos;
\copy bronze.manaos(raw_line) FROM '/Users/jhongarciabarrera/VS - Local/Github/caudales-dw/data/raw/Manaos.csv' WITH (FORMAT text);
\echo 'OK - Manaos'

-- =================================================
-- OBIDOS
-- =================================================
\echo 'Cargando BRONZE - Obidos...'
TRUNCATE bronze.obidos;
\copy bronze.obidos FROM '/Users/jhongarciabarrera/VS - Local/Github/caudales-dw/data/raw/Obidos.csv' CSV HEADER DELIMITER ';' ENCODING 'LATIN1';
\echo 'OK - Obidos'

-- =================================================
-- TABATINGA
-- =================================================
\echo 'Cargando BRONZE - Tabatinga...'
TRUNCATE bronze.tabatinga;
\copy bronze.tabatinga(raw_line) FROM '/Users/jhongarciabarrera/VS - Local/Github/caudales-dw/data/raw/Tabatinga.csv' WITH (FORMAT text);
\echo 'OK - Tabatinga'

-- =================================================
-- TIMBUES (formato especial)
-- =================================================
\echo 'Cargando BRONZE - Timbues...'
TRUNCATE bronze.timbues;
\copy bronze.timbues(raw_line) FROM '/Users/jhongarciabarrera/VS - Local/Github/caudales-dw/data/raw/Timbues.csv' WITH (FORMAT text);
\echo 'OK - Timbues'

\echo '========================================'
\echo 'CARGA BRONZE FINALIZADA CORRECTAMENTE'
\echo '========================================'