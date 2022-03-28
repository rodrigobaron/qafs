from .core import FeatureStore, InFeature, OutFeature
from .storage import LocalStorage

# def FeatureStore(connection_string: str = None, **kwargs):
#     """Method to create Feature Store objects.

#     Parameters
#     ----------
#     connection_string: str
#         SQLAlchemy connection string for feature store metadata database
#         **kwargs: Additional options to be passed to the Feature Store constructor.

#     Returns
#     -------
#     CoreFeatureStore
#         Feature Store object.
#     """
#     return CoreFeatureStore(connection_string=connection_string, **kwargs)
