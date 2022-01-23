
from .core import CoreFeatureStore
from .version import __version__


def FeatureStore(connection_string=None, **kwargs):
    """Method to create Feature Store objects.
    Args:
        connection_string (str): SQLAlchemy connection string for feature store metadata database
        **kwargs: Additional options to be passed to the Feature Store constructor.
    Returns:
        CoreFeatureStore: Feature Store object.
    """
    return CoreFeatureStore(connection_string=connection_string, **kwargs)