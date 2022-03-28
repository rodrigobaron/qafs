import functools

import pandas as pd

try:
    # Allow for a minimal install with no dask/pyarrow
    from dask import dataframe as dd
except ImportError:
    pass


def concat(dfs):
    """Concat dataframes for multiple features."""
    return pd.concat(dfs, join="outer", axis=1).ffill()


def transform(df, func):
    """Transform dataframe using function."""
    transformed = func(df)
    # Make sure output has a single column named 'value'
    if isinstance(transformed, pd.Series) or isinstance(transformed, dd.Series):
        transformed = transformed.to_frame("value")
    if isinstance(df, pd.DataFrame) and not isinstance(transformed, pd.DataFrame):
        raise RuntimeError("Transforms in this namespace should return Pandas dataframes or series")
    if isinstance(df, dd.DataFrame) and not isinstance(transformed, dd.DataFrame):
        raise RuntimeError("Transforms in this namespace should return Dask dataframes or series")
    if len(transformed.columns) != 1:
        raise RuntimeError(
            "Transform function should return a dataframe with a datetime index and single value column"
        )
    transformed.columns = ["value"]
    return transformed