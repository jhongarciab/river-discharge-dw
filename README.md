# River Discharge Data Warehouse

Data warehouse en PostgreSQL para el analisis de **series de tiempo de caudales mensuales** en grandes cuencas de Sudamerica.

## Arquitectura

Pipeline con tres capas (Medallion Architecture):

| Capa | Esquema | Proposito |
|------|---------|-----------|
| **Bronze** | `bronze` | Ingesta cruda de CSVs heterogeneos (sin transformacion) |
| **Silver** | `silver` | Datos limpios, armonizados y agregados mensualmente con imputacion climatologica |
| **Gold** | `gold` | Vista de anomalias estandarizadas (z-scores) sobre la climatologia mensual |

## Estaciones

| Estacion | Rio | Cuenca | Pais |
|----------|-----|--------|------|
| Calamar | Magdalena | Magdalena | Colombia |
| Ciudad Bolivar | Orinoco | Orinoco | Venezuela |
| Manaos | Negro / Amazonas | Amazonas | Brasil |
| Obidos | Amazonas | Amazonas | Brasil |
| Tabatinga | Amazonas | Amazonas | Brasil |
| Timbues | Parana | Parana | Argentina |

## Estructura del proyecto

```
caudales-dw/
├── data/
│   └── raw/                          # CSVs fuente por estacion
│       ├── Calamar.csv
│       ├── CiudadBolivar.csv
│       ├── Manaos.csv
│       ├── Obidos.csv
│       ├── Tabatinga.csv
│       └── Timbues.csv
└── sql/
    ├── 00_init_schemas.sql           # Crear esquemas bronze/silver/gold
    ├── bronze/
    │   ├── 01_ddl.sql                # Tablas de ingesta cruda
    │   └── 02_load_insert.sql        # Carga de CSVs (rutas relativas)
    ├── silver/
    │   ├── 03_ddl.sql                # Tabla de caudales mensuales limpios
    │   └── 04_insert.sql             # Limpieza, agregacion e imputacion
    ├── gold/
    │   └── 05_view_anomalies.sql     # VIEW de anomalias (z-scores)
    └── tests/
        ├── test_silver.sql           # Validaciones de integridad Silver
        └── test_gold.sql             # Validaciones estadisticas Gold
```

## Ejecucion

Todos los scripts se ejecutan desde la raiz del repositorio con `psql`:

```bash
# 1. Crear esquemas
psql -d <base> -f sql/00_init_schemas.sql

# 2. Crear tablas bronze y cargar CSVs
psql -d <base> -f sql/bronze/01_ddl.sql
psql -d <base> -f sql/bronze/02_load_insert.sql

# 3. Crear tabla silver y transformar datos
psql -d <base> -f sql/silver/03_ddl.sql
psql -d <base> -f sql/silver/04_insert.sql

# 4. Crear vista gold de anomalias
psql -d <base> -f sql/gold/05_view_anomalies.sql

# 5. Ejecutar tests de validacion
psql -d <base> -f sql/tests/test_silver.sql
psql -d <base> -f sql/tests/test_gold.sql
```

> **Nota:** `02_load_insert.sql` usa rutas relativas (`./data/raw/...`), por eso es necesario ejecutar `psql` desde la raiz del repositorio.

## Pipeline de datos

### Bronze (ingesta cruda)
Carga los CSVs tal como vienen de cada fuente. Algunas estaciones tienen CSV estructurado (Calamar, Obidos) y otras se ingesan como texto plano (`raw_line`) para parsearse en Silver.

### Silver (limpieza y armonizacion)
Para cada estacion aplica un patron estandar:
1. **Parsing** — extraer campos del formato crudo
2. **Limpieza** — filtros de calidad (regex, rangos fisicos, outliers)
3. **Agregacion** — promedio diario a mensual
4. **Imputacion** — rellenar huecos conocidos con climatologia mensual

### Gold (anomalias)
Vista que calcula z-scores sobre la climatologia mensual:

```
z = (valor_observado - media_mensual) / desviacion_estandar_mensual
```

Al ser una VIEW, se recalcula automaticamente cuando los datos de Silver cambian.

## Tests

Los tests usan bloques `DO $$` con `RAISE EXCEPTION` en caso de fallo:

- **test_silver.sql** — 8 validaciones: integridad, rangos fisicos, conteos minimos
- **test_gold.sql** — 6 validaciones: propiedades estadisticas de z-scores (media ~0, std ~1)

## Tecnologias

- PostgreSQL (ETL y analitica en SQL puro)
- Sin dependencias externas (no Python, no dbt)
