# River Discharge Data Warehouse

This repository implements a **PostgreSQL-based data warehouse** for the analysis of **monthly river discharge time series** across major South American basins.

The pipeline follows a layered architecture:

- **Bronze**: ingestion of raw, heterogeneous CSV files.
- **Silver**: cleaned and harmonized monthly discharge series, including gap filling and quality control.
- **Gold**: analytical products, specifically **standardized monthly discharge anomalies (z-scores)** computed from monthly climatology.

### Rivers and Stations
- Magdalena (Calamar)
- Orinoco (Ciudad Bolívar)
- Amazon (Manaos, Óbidos, Tabatinga)
- Paraná (Timbúes)

### Technologies
- PostgreSQL (SQL-based ETL and analytics)
- Reproducible and auditable pipeline (no Excel dependencies in final layers)

### Purpose
This project provides a solid foundation for **hydrological and climatological analysis**, anomaly detection, and further modeling in external tools.