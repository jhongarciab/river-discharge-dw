-- ==========================================================
-- 00_init_schemas.sql
-- ==========================================================
-- Proposito : Crear los esquemas del data warehouse.
-- Dependencias: Ninguna (primer script del pipeline).
-- Ejecucion :
--   psql -d <base> -f sql/00_init_schemas.sql
-- ==========================================================

CREATE SCHEMA IF NOT EXISTS bronze;   -- Datos crudos tal como llegan de las fuentes
CREATE SCHEMA IF NOT EXISTS silver;   -- Datos limpios, armonizados y agregados mensualmente
CREATE SCHEMA IF NOT EXISTS gold;     -- Productos analiticos (vistas de anomalias)
