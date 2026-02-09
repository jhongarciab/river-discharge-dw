-- ==========================================================
-- 01_ddl.sql  (Bronze)
-- ==========================================================
-- Proposito : Definir las tablas de ingesta cruda (bronze).
--             Cada tabla refleja la estructura original del CSV
--             de su estacion. Donde el CSV es heterogeneo o
--             semi-estructurado se ingesta como raw_line TEXT.
-- Dependencias: 00_init_schemas.sql
-- Ejecucion :
--   psql -d <base> -f sql/bronze/01_ddl.sql
-- ==========================================================

\set ON_ERROR_STOP on

-- ==========================================================
-- CALAMAR (Magdalena) — CSV estructurado con 21 columnas
-- Fuente: IDEAM Colombia, separador ';', encoding LATIN1
-- ==========================================================
DROP TABLE IF EXISTS bronze.calamar;
CREATE TABLE bronze.calamar (
    codigo_estacion    TEXT,
    nombre_estacion    TEXT,
    latitud            TEXT,
    longitud           TEXT,
    altitud            TEXT,
    categoria          TEXT,
    entidad            TEXT,
    area_operativa     TEXT,
    departamento       TEXT,
    municipio          TEXT,
    fecha_instalacion  TEXT,
    fecha_suspension   TEXT,
    id_parametro       TEXT,
    etiqueta           TEXT,
    descripcion_serie  TEXT,
    frecuencia         TEXT,
    fecha              TEXT,
    valor              TEXT,
    grado              TEXT,
    calificador        TEXT,
    nivel_aprobacion   TEXT
);

-- ==========================================================
-- CIUDAD BOLIVAR (Orinoco) — CSV semi-estructurado
-- Se ingesta como linea de texto cruda; se parsea en Silver.
-- Fuente: GRDC / BNO Venezuela, separador ';'
-- ==========================================================
DROP TABLE IF EXISTS bronze.ciudad_bolivar;
CREATE TABLE bronze.ciudad_bolivar (
    raw_line TEXT
);

-- ==========================================================
-- MANAOS (Amazonas / Rio Negro) — CSV semi-estructurado
-- Se ingesta como linea de texto cruda; se parsea en Silver.
-- Fuente: ANA Brasil / GRDC, separador ';'
-- ==========================================================
DROP TABLE IF EXISTS bronze.manaos;
CREATE TABLE bronze.manaos (
    raw_line TEXT
);

-- ==========================================================
-- OBIDOS (Amazonas) — CSV con 5 columnas
-- Fuente: ANA Brasil / GRDC, separador ';', encoding LATIN1
-- ==========================================================
DROP TABLE IF EXISTS bronze.obidos;
CREATE TABLE bronze.obidos (
    id_estacion  TEXT,
    nombre       TEXT,
    fecha        TEXT,
    valor        TEXT,
    origen       TEXT
);

-- ==========================================================
-- TABATINGA (Amazonas) — CSV semi-estructurado
-- Se ingesta como linea de texto cruda; se parsea en Silver.
-- Fuente: ANA Brasil / GRDC, separador ';'
-- ==========================================================
DROP TABLE IF EXISTS bronze.tabatinga;
CREATE TABLE bronze.tabatinga (
    raw_line TEXT
);

-- ==========================================================
-- TIMBUES (Parana) — CSV semi-estructurado
-- Se ingesta como linea de texto cruda; se parsea en Silver.
-- Fuente: INA Argentina, separador ';'
-- ==========================================================
DROP TABLE IF EXISTS bronze.timbues;
CREATE TABLE bronze.timbues (
    raw_line TEXT
);
