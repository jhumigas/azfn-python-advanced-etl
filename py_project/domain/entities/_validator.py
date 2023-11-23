import inspect
import logging
import typing

import pandas as pd
import pandera as pa
import wrapt

from py_project.logger import log_memory_percent_usage

Schemas = typing.Union[pa.schemas.DataFrameSchema, pa.schemas.SeriesSchema]


@log_memory_percent_usage
def validate(schema: Schemas, input_df: pd.DataFrame) -> pd.DataFrame:
    try:
        schema.validate(input_df, lazy=True, inplace=True)
    # Handling exception in case there is a single error
    except pa.errors.SchemaError as exc:
        logging.warning("Data Validation checks failed")
        logging.warning(exc)
        # We remove the row with abnormal values
        # Then we log it, after analysis if we can fix it, we do it prior to the validation
        input_df = input_df[~input_df.index.isin(exc.failure_cases["index"])]
        return input_df
    # Handling exception in case there are multiple errors
    except pa.errors.SchemaErrors as exc:
        logging.warning("Data Validation checks failed")
        logging.warning(exc)
        input_df = input_df[~input_df.index.isin(exc.failure_cases["index"])]
        # Second validation to have the right types, otherwise raise error.
        schema.validate(input_df, lazy=True, inplace=True)
        return input_df
    return input_df


def get_function_argnames(fn: typing.Callable) -> typing.List[str]:
    arg_spec = inspect.getfullargspec(fn).args
    first_arg_is_self = arg_spec[0] == "self"
    is_regular_method = inspect.ismethod(fn) and first_arg_is_self

    if is_regular_method:
        # Don't include "self" / "cls" argument
        arg_spec = arg_spec[1:]
    return arg_spec


def validate_input(schema: Schemas, obj_getter: typing.Optional[typing.Union[str, int]] = None) -> typing.Callable:
    @wrapt.decorator
    def _wrapper(
        wrapped_fn: typing.Callable,
        instance: typing.Union[None, typing.Any],
        args: typing.Tuple[typing.Any, ...],
        kwargs: typing.Dict[str, typing.Any],
    ):
        args = list(args)
        if isinstance(obj_getter, int):
            args[obj_getter] = validate(schema, args[obj_getter])
        elif isinstance(obj_getter, str):
            kwargs[obj_getter] = validate(schema, kwargs[obj_getter])
        elif obj_getter is None and args and len(args) > 0:
            args[0] = validate(schema, args[0])

        return wrapped_fn(*args, **kwargs)

    return _wrapper


def validate_output(schema: Schemas) -> typing.Callable:
    @wrapt.decorator
    def _wrapper(
        wrapped_fn: typing.Callable,
        instance: typing.Union[None, typing.Any],
        args: typing.Tuple[typing.Any, ...],
        kwargs: typing.Dict[str, typing.Any],
    ):
        result = wrapped_fn(*args, **kwargs)
        return validate(schema, result)

    return _wrapper
