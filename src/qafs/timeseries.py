import functools

import pandas as pd


def concat(dfs):
    """Concat dataframes for multiple features."""
    return pd.concat(dfs, join="outer", axis=1).ffill()
