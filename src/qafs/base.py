from abc import ABC
from typing import Dict, List, Optional, Union

import pandas as pd
from pandera import Column


class BaseFeatureStore(ABC):
    """Feature Store implementation contract class."""

    @classmethod
    def _split_name(cls, namespace=None, name=None):
        """Parse namespace and name."""
        if not namespace and name and "/" in name:
            parts = name.split("/")
            namespace, name = parts[0], "/".join(parts[1:])
        return namespace, name

    @classmethod
    def _unpack_list(cls, obj, namespace=None):
        """Extract namespace, name combinations from DataFrame or list
        and return as list of tuples
        """
        if isinstance(obj, str):
            return [cls._split_name(namespace=namespace, name=obj)]
        elif isinstance(obj, pd.DataFrame):
            # DataFrame format must have a name column
            df = obj
            if "name" not in df.columns:
                raise ValueError("DataFrame must have a name column")
            return [(row.get("namespace", namespace), row.get("name")) for _, row in df.iterrows()]
        elif isinstance(obj, list):
            # Could be list of names, of list of dictionaries
            r = []
            for item in obj:
                if isinstance(item, str):
                    r.append(cls._split_name(name=item, namespace=namespace))
                elif isinstance(item, dict):
                    r.append(cls._split_name(namespace=item.get("namespace"), name=item.get("name")))
                else:
                    raise ValueError("List must contain strings or dicts")
            return r
        else:
            raise ValueError("Must supply a string, dataframe or list specifying namespace/name")

    def __init__(self, connection_string=None, url=None, storage_options=None, backend="pandas"):
        """Create a Feature Store, or connect to an existing one.

        Parameters
        ----------
        connection_string: str
            Database connection string.
        url: str
            url of data store.
        storage_options: dict, optinal
            storage options, e.g. access credentials.
        backend: str, optional
            storage backend, see feature_store.storage.available_backends, defaults to `"pandas"`.
        """
        raise NotImplementedError()

    def list_namespaces(
        self, name: Optional[str] = None, namespace: Optional[str] = None, regex: Optional[str] = None
    ):
        """List namespaces in the feature store.

        Search by name or regex query.

        Parameters
        ----------
        name: str, optional
            name of namespace to filter by.
        namespace: str, optional
            same as name.
        regex: str, optional
            regex filter on name.

        Returns
        -------
        pd.DataFrame
            DataFrame of namespaces and metadata.
        """
        raise NotImplementedError()

    def create_namespace(
        self, name: str, description: Optional[str] = None, meta: Optional[Dict] = {}, backend: Optional[str] = None
    ):
        """Create a new namespace in the feature store.

        Parameters
        ----------
        name: str
            name of the namespace.
        description: str, optional
            description for this namespace.
        meta: dict, optional
            key/value pairs of metadata.
        backend: str, optional
            storage backend, see feature_store.storage.available_backends, defaults to `"pandas"`.
        """
        raise NotImplementedError()

    def update_namespace(self, name: str, description: Optional[str] = None, meta: Optional[Dict] = {}):
        """Update a namespace in the feature store.

        Parameters
        ----------
        name: str
            namespace to update.
        description: str, optional
            updated description.
        meta: dict, optional
            updated key/value pairs of metadata.
            To remove metadata, update using `{"key_to_remove": None}`.
        """
        raise NotImplementedError()

    def delete_namespace(self, name: str, delete_data: bool = False):
        """Delete a namespace from the feature store.

        Parameters
        ----------
        name: str
            namespace to be deleted.
        delete_data: bool
            also delete feature data
        """
        raise NotImplementedError()

    def clean_namespace(self, name: str):
        """Removes any data that is not associated with features in the namespace.

        Run this to free up disk space after deleting features

        Parameters
        ----------
        name: str
            namespace to clean
        """
        raise NotImplementedError()

    def list_features(
        self,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        regex: Optional[str] = None,
        friendly: Optional[bool] = None,
    ):
        """List features in the feature store.

        Search by namespace, name and/or regex query

        Parameters
        ----------
        name: str, optional
            name of feature to filter by.
        namespace: str, optional
            namespace to filter by.
        regex: str, optional
            regex filter on name.
        friendly: bool, optional
            simplify output for user.

        Returns
        -------
        pd.DataFrame
            DataFrame of features and metadata.
        """
        raise NotImplementedError()

    def create_feature(
        self,
        name: str,
        check: Column,
        namespace: Optional[str] = None,
        description: Optional[str] = None,
        partition: Optional[str] = None,
        serialized: Optional[bool] = None,
        transform: Optional[str] = None,
        meta: Optional[Dict] = {},
    ):
        """Create a new feature in the feature store.

        Parameters
        ----------
        name: str
            name of the feature
        check: Column
            Data validation check
        namespace: str, optional
            namespace which should hold this feature.
        description: str, optional
            description for this namespace.
        partition: str, optional
            partitioning of stored timeseries (default: `"date"`).
        serialized: bool, optional
            if `True`, converts values to JSON strings before saving,
            which can help in situations where the format/schema of the data changes
            over time.
        transform: str, optional
            pickled function code for feature transforms.
        meta: dict, optional
            key/value pairs of metadata.
        """
        raise NotImplementedError()

    def clone_feature(
        self,
        name: str,
        namespace: Optional[str] = None,
        from_name: Optional[str] = None,
        from_namespace: Optional[str] = None,
    ):
        """Create a new feature by cloning an existing one.

        Parameters
        ----------
        name: str
            name of the feature.
        namespace: str, optional
            namespace which should hold this feature.
        from_name: str
            the name of the existing feature to copy from.
        from_namespace: str, optional
            namespace of the existing feature.
        """
        raise NotImplementedError()

    def delete_feature(self, name: str, namespace: Optional[str] = None, delete_data: Optional[bool] = False):
        """Delete a feature from the feature store.

        Parameters
        ----------
        name: str
            name of feature to delete.
        namespace str, optional
            namespace, if not included in feature name.
        delete_data: bool, optional
            if set to `True` will delete underlying stored data
            for this feature, otherwise default behaviour is to delete the feature store
            metadata but leave the stored timeseries values intact.
        """
        raise NotImplementedError()

    def update_feature(
        self,
        name: str,
        namespace: Optional[str] = None,
        description: Optional[str] = None,
        transform: Optional[str] = None,
        check: Optional[Column] = None,
        meta: Optional[Dict] = {},
    ):
        """Update a feature in the feature store.

        Parameters
        ----------
        name: str
            feature to update.
        namespace: str, optional
            namespace, if not included in feature name.
        description: str, optional
            updated description.
        transform: str, optional
            pickled function code for feature transforms.
        check: Column, optional
            data validation check.
        meta: dict, optional
            updated key/value pairs of metadata.
            To remove metadata, update using `{"key_to_remove": None}`.
        """
        raise NotImplementedError()

    def transform(self, name: str, check: Column, namespace: Optional[str] = None, from_features: List = []):
        """Decorator for creating/updating virtual (transformed) features.

        Use this on a function that accepts a dataframe input and returns an output dataframe
        of tranformed values.

        Parameters
        ----------
        name: str
            feature to update.
        check: Column
            data validation check.
        namespace: str, optional
            namespace, if not included in feature name.
        from_features: list
            list of features which should be transformed by this one
        """
        raise NotImplementedError()

    def load_dataframe(
        self,
        features: Union[str, list, pd.DataFrame],
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        freq: Optional[str] = None,
        time_travel: Optional[str] = None,
    ):
        """Load a DataFrame of feature values from the feature store.

        Parameters
        ----------
        features: Union[str, list, pd.DataFrame]
            name of feature to load, or list/DataFrame of feature namespaces/name.
        from_date: datetime, optional
            start date to load timeseries from, defaults to everything.
        to_date datetime, optional
            end date to load timeseries to, defaults to everything.
        freq: str, optional
            frequency interval at which feature values should be sampled.
        time_travel str, optional
            timedelta string, indicating that time-travel should be applied to the
            returned timeseries values, useful in forecasting applications.

        Returns
        -------
        Union[pd.DataFrame, dask.DataFrame]
            depending on which backend was specified in the feature store.
        """
        raise NotImplementedError()

    def save_dataframe(self, df: pd.DataFrame, name: Optional[str] = None, namespace: Optional[str] = None):
        """Save a DataFrame of feature values to the feature store.

        Parameters
        ----------
        df: pd.DataFrame
            DataFrame of feature values.
            Must have a `time` column or DateTimeIndex of time values.
            Optionally include a `created_time` column (defaults to `utcnow()` if omitted).
            For a single feature: a `value` column, or column header of feature `namespace/name`.
            For multiple features name the columns using `namespace/name`.
        name: str, optional
            name of feature, if not included in DataFrame column name.
        namespace: str, optional
            namespace, if not included in DataFrame column name.
        """
        raise NotImplementedError()

    def last(self, features):
        """Fetch the last value of one or more features.
        Parameters
        ----------
        features: Union[str, list, pd.DataFrame]
            feature or features to fetch.
        Returns
        -------
        Dict
            dictionary of name, last value pairs.
        """
        raise NotImplementedError()

    def create_task(self):
        """Create a scheduled task to update the feature store."""
        raise NotImplementedError()

    def update_task(self):
        """Update a task."""
        raise NotImplementedError()

    def delete_task(self):
        """Delete a task."""
        raise NotImplementedError()
