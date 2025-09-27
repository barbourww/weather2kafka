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



CREATE TABLE IF NOT EXISTS laddms.weather_radar (
    write_time          TIMESTAMPTZ NOT NULL,
    generate_time       TIMESTAMPTZ,
    x_easting           JSON,
    y_northing          JSON,
    radar_array         JSON,
    center_lat          DOUBLE PRECISION,
    center_lon          DOUBLE PRECISION,
    range_miles         REAL,
    utm_zone_epsg       INTEGER
);

COMMENT ON COLUMN laddms.weather_radar.generate_time IS 'Reported radar generation time from NOAA.';
COMMENT ON COLUMN laddms.weather_radar.x_easting IS 'Easting (UTM) coordinates of x-dimension of radar data.';
COMMENT ON COLUMN laddms.weather_radar.y_northing IS 'Northing (UTM) coordinates of y-dimension of radar data.';
COMMENT ON COLUMN laddms.weather_radar.radar_array IS 'RGBA array with dimensions (M, N, 4), where M=rows, N=cols; ordered from image upper left.';

SELECT create_hypertable(
    'laddms.weather_radar',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);

