from typing import Callable

import pandas as pd
import pandera as pa

from py_project.domain.entities._validator import validate_input, validate_output

WEATHER_RAW_TIMESTAMP_COLUMN_NAME = "Formatted Date"
WEATHER_RAW_TEMPERATURE_COLUMN_NAME = "Temperature (C)"
WEATHER_RAW_HUMIDITY_COLUMN_NAME = "Humidity"
WEATHER_RAW_WIND_SPEED_COLUMN_NAME = "Wind Speed (km/h)"
WEATHER_RAW_WIND_BEARING_COLUMN_NAME = "Wind Bearing (degrees)"
WEATHER_RAW_VISIBILITY_COLUMN_NAME = "Visibility (km)"
WEATHER_RAW_PRESSURE_COLUMN_NAME = "Pressure (millibars)"

WEATHER_TIMESTAMP_COLUMN_NAME = "timestamp"
WEATHER_TEMPERATURE_COLUMN_NAME = "temperature_c"
WEATHER_HUMIDITY_COLUMN_NAME = "humidity_percent"
WEATHER_WIND_SPEED_COLUMN_NAME = "wind_speed_kmh"
WEATHER_WIND_BEARING_COLUMN_NAME = "wind_bearing_deg"
WEATHER_VISIBILITY_COLUMN_NAME = "visibility_km"
WEATHER_PRESSURE_COLUMN_NAME = "pressure_mbar"

WEATHER_WIND_POWER = "wind_power_kw"

WEATHER_RAW_COLUMNS = [
    WEATHER_RAW_TIMESTAMP_COLUMN_NAME,
    WEATHER_RAW_TEMPERATURE_COLUMN_NAME,
    WEATHER_RAW_HUMIDITY_COLUMN_NAME,
    WEATHER_RAW_WIND_SPEED_COLUMN_NAME,
    WEATHER_RAW_WIND_BEARING_COLUMN_NAME,
    WEATHER_RAW_VISIBILITY_COLUMN_NAME,
    WEATHER_RAW_PRESSURE_COLUMN_NAME,
]

RAW_NORMALIZED_COLUMN_MAPPING = {
    WEATHER_RAW_TIMESTAMP_COLUMN_NAME: WEATHER_TIMESTAMP_COLUMN_NAME,
    WEATHER_RAW_TEMPERATURE_COLUMN_NAME: WEATHER_TEMPERATURE_COLUMN_NAME,
    WEATHER_RAW_HUMIDITY_COLUMN_NAME: WEATHER_HUMIDITY_COLUMN_NAME,
    WEATHER_RAW_WIND_SPEED_COLUMN_NAME: WEATHER_WIND_SPEED_COLUMN_NAME,
    WEATHER_RAW_WIND_BEARING_COLUMN_NAME: WEATHER_WIND_BEARING_COLUMN_NAME,
    WEATHER_RAW_VISIBILITY_COLUMN_NAME: WEATHER_VISIBILITY_COLUMN_NAME,
    WEATHER_RAW_PRESSURE_COLUMN_NAME: WEATHER_PRESSURE_COLUMN_NAME,
}

NORMALIZED_COLUMN_TYPES = {
    WEATHER_TIMESTAMP_COLUMN_NAME: "datetime64[ns, UTC]",
    WEATHER_TEMPERATURE_COLUMN_NAME: "double",
    WEATHER_HUMIDITY_COLUMN_NAME: "double",
    WEATHER_WIND_SPEED_COLUMN_NAME: "double",
    WEATHER_WIND_BEARING_COLUMN_NAME: "double",
    WEATHER_VISIBILITY_COLUMN_NAME: "double",
    WEATHER_PRESSURE_COLUMN_NAME: "double",
}


class RawWeatherMetricsSchema(pa.SchemaModel):
    timestamp: pa.typing.Series[pa.typing.String] = pa.Field(alias=WEATHER_RAW_TIMESTAMP_COLUMN_NAME, coerce=True)
    temperature: pa.typing.Series[pa.typing.String] = pa.Field(alias=WEATHER_RAW_TEMPERATURE_COLUMN_NAME, coerce=True)
    humidity: pa.typing.Series[pa.typing.String] = pa.Field(alias=WEATHER_RAW_HUMIDITY_COLUMN_NAME, coerce=True)
    wind_speed: pa.typing.Series[pa.typing.String] = pa.Field(alias=WEATHER_RAW_WIND_SPEED_COLUMN_NAME, coerce=True)
    wind_bearing: pa.typing.Series[pa.typing.String] = pa.Field(alias=WEATHER_RAW_WIND_BEARING_COLUMN_NAME, coerce=True)
    visibility: pa.typing.Series[pa.typing.String] = pa.Field(alias=WEATHER_RAW_VISIBILITY_COLUMN_NAME, coerce=True)
    pressure: pa.typing.Series[pa.typing.String] = pa.Field(alias=WEATHER_RAW_PRESSURE_COLUMN_NAME, coerce=True)


class NormalizedWeatherMetricsSchema(pa.SchemaModel):
    timestamp: pa.typing.Series[pd.DatetimeTZDtype] = pa.Field(
        alias=WEATHER_TIMESTAMP_COLUMN_NAME, dtype_kwargs={"unit": "ns", "tz": "UTC"}, coerce=True,
    )
    temperature: pa.typing.Series[pa.typing.Float64] = pa.Field(alias=WEATHER_TEMPERATURE_COLUMN_NAME, coerce=True)
    humidity: pa.typing.Series[pa.typing.Float64] = pa.Field(alias=WEATHER_HUMIDITY_COLUMN_NAME, coerce=True)
    wind_speed: pa.typing.Series[pa.typing.Float64] = pa.Field(alias=WEATHER_WIND_SPEED_COLUMN_NAME, coerce=True)
    wind_bearing: pa.typing.Series[pa.typing.Float64] = pa.Field(alias=WEATHER_WIND_BEARING_COLUMN_NAME, coerce=True)
    visibility: pa.typing.Series[pa.typing.Float64] = pa.Field(alias=WEATHER_VISIBILITY_COLUMN_NAME, coerce=True)
    pressure: pa.typing.Series[pa.typing.Float64] = pa.Field(alias=WEATHER_PRESSURE_COLUMN_NAME, coerce=True)


class ComputedWeatherMetricsSchema(NormalizedWeatherMetricsSchema):
    wind_power_kw: pa.typing.Series[pa.typing.Float64] = pa.Field(alias=WEATHER_WIND_POWER, coerce=True)


@validate_output(RawWeatherMetricsSchema)
def transform_to_raw_metrics(input_df: pa.typing.DataFrame) -> pa.typing.DataFrame[RawWeatherMetricsSchema]:
    transformed_df = input_df.copy()
    transformed_df = transformed_df[WEATHER_RAW_COLUMNS]
    return transformed_df


def convert_column_to_datetime(input_df: pd.DataFrame, time_column: str):
    output_df = input_df.copy(deep=True)
    output_df[time_column] = pd.to_datetime(output_df[time_column], utc=True)
    return output_df


@validate_input(RawWeatherMetricsSchema)
@validate_output(NormalizedWeatherMetricsSchema)
def transform_from_raw_to_normalized_metrics(
    input_df: pa.typing.DataFrame[RawWeatherMetricsSchema],
) -> pa.typing.DataFrame[NormalizedWeatherMetricsSchema]:

    metrics_df = input_df.rename(columns=RAW_NORMALIZED_COLUMN_MAPPING)
    metrics_df = convert_column_to_datetime(input_df=metrics_df, time_column=WEATHER_TIMESTAMP_COLUMN_NAME)
    metrics_df = metrics_df.astype(NORMALIZED_COLUMN_TYPES)
    return metrics_df


@validate_input(NormalizedWeatherMetricsSchema)
@validate_output(ComputedWeatherMetricsSchema)
def transform_from_normalized_to_computed_metrics(
    input_df: pa.typing.DataFrame[NormalizedWeatherMetricsSchema],
    add_computed_metrics_fn: Callable[
        [pa.typing.DataFrame[NormalizedWeatherMetricsSchema]], pa.typing.DataFrame[ComputedWeatherMetricsSchema]
    ],
) -> pa.typing.DataFrame[ComputedWeatherMetricsSchema]:
    return add_computed_metrics_fn(input_df)
