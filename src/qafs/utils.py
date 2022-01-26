import base64
import json
import logging
from typing import Callable

import cloudpickle


def is_jsonable(x):
    """verify if object can be converted to json.

    Parameters
    ----------
    x: Any
        input object.

    Returns
    -------
    bool
        if object can be converted to json.
    """
    try:
        json.dumps(x)
        return True
    except Exception:
        return False


def serialize(func: Callable):
    """Serialize function to base64.

    Parameters
    ----------
    func: Callable
        input function.

    Returns
    -------
    str
        base64 serialized function.
    """
    return base64.b64encode(cloudpickle.dumps(func)).decode("utf-8")


def deserialize(func_string: str):
    """Deserialize base64 string to function.

    Parameters
    ----------
    func_string: str
        input base64 function.

    Returns
    -------
    Callable
        deserialized function.
    """
    return cloudpickle.loads(base64.b64decode(func_string.encode("utf-8")))


def setup_logging(level: int = logging.INFO) -> None:
    """Setup the logging to desired level and format the message.
    This filter the logging by the level and format the message.

    Parameters
    ----------
    level: int
        The logging level.
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        level=level,
    )
