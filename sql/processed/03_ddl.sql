DROP TABLE IF EXISTS silver.flow_monthly;

CREATE TABLE silver.flow_monthly (
    year  INT NOT NULL,
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),

    calamar_monthly        DOUBLE PRECISION,
    bolivar_monthly        DOUBLE PRECISION,
    manaos_monthly         DOUBLE PRECISION,
    obidos_monthly         DOUBLE PRECISION,
    tabatinga_monthly      DOUBLE PRECISION,
    timbues_monthly        DOUBLE PRECISION,

    CONSTRAINT pk_flow_monthly
        PRIMARY KEY (year, month)
);