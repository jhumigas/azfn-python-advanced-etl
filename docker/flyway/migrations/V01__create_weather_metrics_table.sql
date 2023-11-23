
CREATE TABLE IF NOT EXISTS schema_metrics.weather_metrics (
   timestamp TIMESTAMP WITH TIME ZONE,
   temperature_c NUMERIC,
   humidity_percent NUMERIC,
   wind_speed_kmh NUMERIC,
   wind_bearing_deg NUMERIC,
   visibility_km NUMERIC,
   pressure_mbar NUMERIC,
   wind_power_kw NUMERIC
);

-- SELECT create_hypertable('schema_metrics.weather_metrics','timestamp');
