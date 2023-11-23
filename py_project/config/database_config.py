DEFAULT_DATABASE = "main"

DEFAULT_SCHEMA = "schema_metrics"

WRITE_MODE_APPEND = "append"
WRITE_MODE_TRUNCATE_THEN_APPEND = "truncate_then_append"
WRITE_MODE_UPSERT = "upsert"
WRITE_MODE_REPLACE = "replace"

# --------------------------------------- #
#             Weather Tables              #
# --------------------------------------- #
WEATHER_METRICS_TABLE_NAME = "weather_metrics"

WEATHER_STATION_ID_COLUMN_NAME = "station_id"
WEATHER_TIMESTAMP_COLUMN_NAME = "timestamp"
WEATHER_TEMPERATURE_COLUMN_NAME = "temperature_c"
WEATHER_PRESSURE_COLUMN_NAME = "pressure_hpa"
WEATHER_MEASUREMENT_COLUMN_NAME = "measurement_id"
WEATHER_WIND_DIRECTION_COLUMN_NAME = "wind_direction_deg"
WEATHER_WIND_SPEED_COLUMN_NAME = "wind_speed_ms"
WEATHER_PLUVIOMETRY_COLUMN_NAME = "pluviometry_mm"
WEATHER_HUMIDITY_PERCENTAGE_COLUMN_NAME = "humidity_percent"
WEATHER_SOLAR_RAD_COLUMN_NAME = "solar_radiation_kwh_m2"
