import unittest
from unittest.mock import patch

import pandas as pd
import pandera as pa
import pytest

from py_project.domain.entities import _validator

TESTED_MODULE = "py_project.domain.entities._validator"


class TestValidationHelpers(unittest.TestCase):
    def test_should_get_function_args_for_regular_function(self):
        # Given
        def given_fn(arg):
            return arg

        # When
        output_args = _validator.get_function_argnames(given_fn)
        # Then
        assert output_args == ["arg"]

    def test_should_not_get_self_arg_for_class_function(self):
        # Given
        class GivenClass:
            def given_fn(self, arg):
                return arg

        # When
        output_args = _validator.get_function_argnames(GivenClass().given_fn)
        # Then
        assert output_args == ["arg"]

    @pytest.fixture(scope="function", autouse=True)
    def prepare_schema(self):
        class GivenSchema(pa.SchemaModel):
            year: pa.typing.Series[int] = pa.Field(gt=2000, coerce=True)
            month: pa.typing.Series[int] = pa.Field(ge=1, le=12, coerce=True)
            day: pa.typing.Series[int] = pa.Field(ge=0, le=365, coerce=True)

        self.given_schema = GivenSchema

    def test_should_keep_all_rows_in_valid_dataframe(self):
        # Given
        given_valid_df = pd.DataFrame(
            {
                "year": ["2001", "2002", "2003"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )
        # When
        expected_df = given_valid_df.copy()
        validated_df = _validator.validate(self.given_schema, given_valid_df)
        expected_df = expected_df.astype("int64")
        # Then
        pd.testing.assert_frame_equal(validated_df, expected_df)

    def test_should_filter_invalid_row_in_invalid_dataframe(self):
        # Given

        given_invalid_df = pd.DataFrame(
            {
                "year": ["2001", "2002", "1999"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )
        # When
        expected_df = given_invalid_df.iloc[:-1].astype("int64")
        validated_df = _validator.validate(self.given_schema, given_invalid_df)

        # Then
        pd.testing.assert_frame_equal(validated_df, expected_df)

    @patch(f"{TESTED_MODULE}.validate", return_value=pd.DataFrame({}))
    def test_validate_input_decorator_should_filter_invalid_row_in_first_arg_by_default(self, validate_mock):
        # Given
        @_validator.validate_input(schema=self.given_schema)
        def given_function(
            first_df: pa.typing.DataFrame[self.given_schema], second_df: pa.typing.DataFrame[self.given_schema]
        ):
            return (first_df, second_df)

        given_first_df = pd.DataFrame(
            {
                "year": ["2001", "2002", "1999"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )
        given_second_df = pd.DataFrame(
            {
                "year": ["2001", "2002", "1999"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )
        # When
        output_result = given_function(given_first_df, given_second_df)

        # Then
        validate_mock.assert_called_once()
        assert len(given_first_df) > len(output_result[0])
        assert len(given_second_df) == len(output_result[1])

    @patch(f"{TESTED_MODULE}.validate", return_value=pd.DataFrame({}))
    def test_validate_input_decorator_should_filter_invalid_row_in_named_arg(self, validate_mock):
        # Given

        @_validator.validate_input(schema=self.given_schema, obj_getter="second_df")
        def given_function(
            first_df: pa.typing.DataFrame[self.given_schema], second_df: pa.typing.DataFrame[self.given_schema]
        ):
            return (first_df, second_df)

        given_first_df = pd.DataFrame(
            {
                "year": ["2001", "2002", "1999"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )
        given_second_df = pd.DataFrame(
            {
                "year": ["2001", "2002", "1999"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )
        # When
        output_result = given_function(first_df=given_first_df, second_df=given_second_df)

        # Then
        validate_mock.assert_called_once()
        assert len(given_first_df) == len(output_result[0])
        assert len(given_second_df) > len(output_result[1])

    @patch(f"{TESTED_MODULE}.validate", return_value=pd.DataFrame({}))
    def test_validate_input_decorator_should_filter_invalid_row_in_second_arg(self, validate_mock):
        # Given

        @_validator.validate_input(schema=self.given_schema, obj_getter=1)
        def given_function(
            first_df: pa.typing.DataFrame[self.given_schema], second_df: pa.typing.DataFrame[self.given_schema]
        ):
            return (first_df, second_df)

        given_first_df = pd.DataFrame(
            {
                "year": ["2001", "2002", "1999"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )
        given_second_df = pd.DataFrame(
            {
                "year": ["2001", "2002", "1999"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )
        # When
        output_result = given_function(given_first_df, given_second_df)

        # Then
        validate_mock.assert_called_once()
        assert len(given_first_df) == len(output_result[0])
        assert len(given_second_df) > len(output_result[1])

    @patch(f"{TESTED_MODULE}.validate", return_value=pd.DataFrame({}))
    def test_validate_output_decorator_should_filter_invalid_row_in_output(self, validate_mock):
        # Given

        @_validator.validate_output(schema=self.given_schema)
        def given_function(
            first_df: pa.typing.DataFrame[self.given_schema], second_df: pa.typing.DataFrame[self.given_schema]
        ):
            return first_df + second_df

        given_first_df = pd.DataFrame(
            {
                "year": [2001, 2002, 400],
                "month": [3, 3, 3],
                "day": [100, 100, 100],
            }
        )
        given_second_df = pd.DataFrame(
            {
                "year": [2001, 2002, 400],
                "month": [3, 6, 12],
                "day": [100, 100, 100],
            }
        )
        # When
        output_result = given_function(first_df=given_first_df, second_df=given_second_df)

        # Then
        validate_mock.assert_called_once()
        assert len(output_result) == 0
