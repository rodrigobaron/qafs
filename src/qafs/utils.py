import json
import logging
import base64
import cloudpickle


def is_jsonable(x):
    try:
        json.dumps(x)
        return True
    except:
        return False


def serialize(func):
    return base64.b64encode(cloudpickle.dumps(func)).decode("utf-8")


def deserialize(func_string):
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