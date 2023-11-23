import logging
import os
import types
import typing

import psutil
import wrapt

logger = logging.getLogger(__name__)


def compute_memory_percent_usage() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_percent()


@wrapt.decorator
def log_memory_percent_usage(
    wrapped_fn: types.FunctionType,
    instance: typing.Union[None, typing.Any],
    args: typing.Tuple[typing.Any, ...],
    kwargs: typing.Dict[str, typing.Any],
):
    logger.debug(
        f"Memory use before calling {wrapped_fn.__module__}.{wrapped_fn.__name__}: "
        f"{compute_memory_percent_usage():.3f} %"
    )
    return_value = wrapped_fn(*args, **kwargs)
    logger.debug(
        f"Memory use after calling {wrapped_fn.__module__}.{wrapped_fn.__name__}: "
        f"{compute_memory_percent_usage():.3f} %"
    )
    return return_value
