CREATE TABLE IF NOT EXISTS laddms.weather_conditions (
    write_time          TIMESTAMPTZ NOT NULL,
    start_time          TIMESTAMPTZ NOT NULL,
    end_time            TIMESTAMPTZ,
    generate_time       TIMESTAMPTZ,
    is_daytime          BOOL,
    temperature         REAL,
    feels_like          REAL,
    humidity            REAL,
    short_forecast      TEXT,
    precip_chance       REAL,
    precip_last3hours   REAL
);

COMMENT ON COLUMN laddms.weather_conditions.temperature IS 'Units: degrees Fahrenheit';
COMMENT ON COLUMN laddms.weather_conditions.feels_like IS 'Units: degrees Fahrenheit';
COMMENT ON COLUMN laddms.weather_conditions.humidity IS 'Units: percent (relative humidity)';
COMMENT ON COLUMN laddms.weather_conditions.humidity IS 'Units: percent';
COMMENT ON COLUMN laddms.weather_conditions.precip_chance IS 'Units: percent';
COMMENT ON COLUMN laddms.weather_conditions.precip_last3hours IS 'Units: inches';

SELECT create_hypertable(
    'laddms.weather_conditions',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);

