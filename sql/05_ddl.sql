gold.flow_monthly_anomalies (
  year INT,
  month INT,

  calamar_anomaly     DOUBLE PRECISION,
  bolivar_anomaly     DOUBLE PRECISION,
  manaos_anomaly      DOUBLE PRECISION,
  obidos_anomaly      DOUBLE PRECISION,
  tabatinga_anomaly   DOUBLE PRECISION,
  timbues_anomaly     DOUBLE PRECISION,

  PRIMARY KEY (year, month)
)