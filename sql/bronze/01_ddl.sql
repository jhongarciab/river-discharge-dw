-- Calamar
DROP TABLE IF EXISTS bronze.calamar;
CREATE TABLE IF NOT EXISTS bronze.calamar (
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

-- Ciudad Bolivar
DROP TABLE IF EXISTS bronze.ciudad_bolivar;
CREATE TABLE bronze.ciudad_bolivar (
  raw_line TEXT
);

-- Manaos
DROP TABLE IF EXISTS bronze.manaos;
CREATE TABLE IF NOT EXISTS bronze.manaos (
  raw_line TEXT
);

-- Obidos
DROP TABLE IF EXISTS bronze.obidos;
CREATE TABLE IF NOT EXISTS bronze.obidos (
  id_estacion    TEXT,
  nombre         TEXT,
  fecha          TEXT,
  valor          TEXT,
  origen         TEXT
);

-- Tabatinga
DROP TABLE IF EXISTS bronze.tabatinga;
CREATE TABLE IF NOT EXISTS bronze.tabatinga (
  raw_line TEXT
);

-- Timbues
DROP TABLE IF EXISTS bronze.timbues;
CREATE TABLE IF NOT EXISTS bronze.timbues (
  raw_line TEXT
);