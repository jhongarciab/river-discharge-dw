# CLAUDE.md

## Resumen

Data warehouse de caudales fluviales en PostgreSQL. Arquitectura medallion (Bronze -> Silver -> Gold) implementada en SQL puro. Analiza series de tiempo mensuales de 6 estaciones en Sudamerica.

## Stack

- **Base de datos**: PostgreSQL
- **Lenguaje**: SQL puro (sin dbt, sin Python)
- **Ejecucion**: `psql` desde la raiz del repositorio
- **Datos**: CSVs con separador `;`, algunos con encoding LATIN1

## Estructura

```
├── data/raw/                         # CSVs fuente (6 estaciones)
└── sql/
    ├── 00_init_schemas.sql           # Esquemas bronze/silver/gold
    ├── bronze/
    │   ├── 01_ddl.sql                # Tablas de ingesta cruda
    │   └── 02_load_insert.sql        # Carga CSVs (rutas relativas)
    ├── silver/
    │   ├── 03_ddl.sql                # Tabla mensual limpia + COMMENT ON
    │   └── 04_insert.sql             # Parsing, limpieza, agregacion, imputacion
    ├── gold/
    │   └── 05_view_anomalies.sql     # VIEW de z-scores
    └── tests/
        ├── test_silver.sql           # 8 asserts de integridad
        └── test_gold.sql             # 6 asserts estadisticos
```

## Ejecucion

Scripts numerados secuencialmente: `00` -> `01` -> `02` -> `03` -> `04` -> `05`. Ejecutar siempre desde la raiz del repo (las rutas de `\copy` son relativas). Scripts idempotentes (DROP/TRUNCATE).

## Convenciones SQL

### Nombres
- **Esquemas**: singular lowercase (`bronze`, `silver`, `gold`)
- **Tablas**: snake_case (`flow_monthly`)
- **Columnas**: `{estacion}_monthly` en Silver, `{estacion}_anomaly` en Gold
- **CTEs**: nombres descriptivos en espanol (`parsing`, `limpieza`, `mensual`, `climatologia`, `imputacion`, `serie_final`)

### Estilo
- Indentacion 4 espacios
- Headers de seccion: `-- ========== [Titulo] ==========`
- Progreso: `\echo 'OK - [Estacion]'`
- Error handling: `\set ON_ERROR_STOP on`
- Upserts: `ON CONFLICT (year, month) DO UPDATE SET ...`

### Patrones
- Parsing de fechas: `EXTRACT(YEAR FROM fecha::date)::int`, `to_timestamp(campo, 'DD/MM/YYYY HH24:MI')`
- Texto semi-estructurado: `split_part(raw_line, ';', N)`, `REPLACE(campo, ',', '.')` para decimales europeos
- Filtro de headers: `raw_line ~ '^[0-9]'`
- Agregacion: `AVG()` diario a mensual, `STDDEV_SAMP()` para z-scores
- Pivoteo: `MAX(valor) FILTER (WHERE station = 'X')`
- Imputacion: climatologia mensual para huecos conocidos

## Pipeline

- **Bronze**: CSVs crudos sin transformar (algunos como `raw_line TEXT`)
- **Silver**: `silver.flow_monthly` — caudales mensuales (m3/s) limpios con imputacion climatologica
- **Gold**: `gold.flow_monthly_anomalies` — VIEW de z-scores que se recalcula automaticamente

## Tests

Ejecutar despues del pipeline. Formato `DO $$ ... RAISE EXCEPTION` (fallo) / `RAISE NOTICE` (exito):
- **test_silver.sql**: sin duplicados, sin negativos, rangos fisicos, conteos minimos
- **test_gold.sql**: media ~0, std ~1, sin extremos |z|>5, NULLs consistentes

## Notas

- Los nombres de estacion aparecen en Bronze DDL, Silver inserts y Gold VIEW — mantenerlos sincronizados al agregar estaciones
- Las reglas de calidad son especificas por fuente (umbrales, filtros de fecha, huecos conocidos)
- `data/processed/` esta en `.gitignore` (outputs se regeneran)
