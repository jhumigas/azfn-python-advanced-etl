import unittest

import pandas as pd

from py_project.domain.entities import weather_data_handler

TESTED_MODULE = "py_project.domain.entities.weather_data_handler"


class TestWeatherDataHandler(unittest.TestCase):
    def test_should_transform_to_raw_metrics(self):
        # Given
        given_input_df = pd.DataFrame(
            {
                weather_data_handler.WEATHER_RAW_TIMESTAMP_COLUMN_NAME: [
                    "2022-05-26 18:00:00+0000",
                    "2022-05-26 19:00:00+0000",
                ],
                weather_data_handler.WEATHER_RAW_TEMPERATURE_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_HUMIDITY_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_WIND_SPEED_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_WIND_BEARING_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_VISIBILITY_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_PRESSURE_COLUMN_NAME: ["0.1", "0.2"],
                "given_additional_column": [1, 2],
            }
        )

        # When
        output_df = weather_data_handler.transform_to_raw_metrics(given_input_df)
        output_columns = output_df.columns.to_list()
        # Then
        assert len(given_input_df) == len(output_df)
        assert set(weather_data_handler.WEATHER_RAW_COLUMNS) == set(output_columns)

    def test_should_convert_column_to_datetime(self):
        # Given
        given_timestamp_column = "given_timestamp_column"
        given_df = pd.DataFrame({given_timestamp_column: ["2022-05-26 18:00:00+0000", "2022-05-26 19:00:00+0000"]})

        # When
        output_df = weather_data_handler.convert_column_to_datetime(
            input_df=given_df, time_column=given_timestamp_column
        )

        # Then
        expected_df = pd.DataFrame(
            {given_timestamp_column: pd.to_datetime(["2022-05-26 18:00:00+0000", "2022-05-26 19:00:00+0000"])}
        )
        assert output_df[given_timestamp_column].dtypes.name == "datetime64[ns, UTC]"
        pd.testing.assert_frame_equal(expected_df, output_df)

    def test_should_transform_from_raw_to_normalized_metrics(self):
        # Given
        given_input_df = pd.DataFrame(
            {
                weather_data_handler.WEATHER_RAW_TIMESTAMP_COLUMN_NAME: [
                    "2022-05-26 18:00:00+0000",
                    "2022-05-26 19:00:00+0000",
                ],
                weather_data_handler.WEATHER_RAW_TEMPERATURE_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_HUMIDITY_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_WIND_SPEED_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_WIND_BEARING_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_VISIBILITY_COLUMN_NAME: ["0.1", "0.2"],
                weather_data_handler.WEATHER_RAW_PRESSURE_COLUMN_NAME: ["0.1", "0.2"],
            }
        )

        expected_output_df = pd.DataFrame(
            {
                weather_data_handler.WEATHER_TIMESTAMP_COLUMN_NAME: pd.to_datetime(
                    ["2022-05-26 18:00:00+0000", "2022-05-26 19:00:00+0000"]
                ),
                weather_data_handler.WEATHER_TEMPERATURE_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_HUMIDITY_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_WIND_SPEED_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_WIND_BEARING_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_VISIBILITY_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_PRESSURE_COLUMN_NAME: [0.1, 0.2],
            }
        )

        # When
        output_df = weather_data_handler.transform_from_raw_to_normalized_metrics(given_input_df)
        # Then
        pd.testing.assert_frame_equal(output_df, expected_output_df, check_like=True)

    def test_should_transform_from_normalized_to_computed_metrics(self):
        # Given
        given_input_df = pd.DataFrame(
            {
                weather_data_handler.WEATHER_TIMESTAMP_COLUMN_NAME: pd.to_datetime(
                    ["2022-05-26 18:00:00+0000", "2022-05-26 19:00:00+0000"]
                ),
                weather_data_handler.WEATHER_TEMPERATURE_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_HUMIDITY_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_WIND_SPEED_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_WIND_BEARING_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_VISIBILITY_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_PRESSURE_COLUMN_NAME: [0.1, 0.2],
            }
        )

        def given_add_computed_metrics_fn(input_df: pd.DataFrame):
            input_df[weather_data_handler.WEATHER_WIND_POWER] = 0.0
            return input_df

        expected_output_df = pd.DataFrame(
            {
                weather_data_handler.WEATHER_TIMESTAMP_COLUMN_NAME: pd.to_datetime(
                    ["2022-05-26 18:00:00+0000", "2022-05-26 19:00:00+0000"]
                ),
                weather_data_handler.WEATHER_TEMPERATURE_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_HUMIDITY_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_WIND_SPEED_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_WIND_BEARING_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_VISIBILITY_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_PRESSURE_COLUMN_NAME: [0.1, 0.2],
                weather_data_handler.WEATHER_WIND_POWER: [0.0, 0.0],
            }
        )

        # When
        output_df = weather_data_handler.transform_from_normalized_to_computed_metrics(
            input_df=given_input_df, add_computed_metrics_fn=given_add_computed_metrics_fn
        )
        # Then
        pd.testing.assert_frame_equal(output_df, expected_output_df, check_like=True)
