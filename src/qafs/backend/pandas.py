import posixpath
import warnings
import os

import dask
import dask.dataframe as dd
import fsspec
import numpy as np
import pandas as pd
import pyarrow as pa

from ._base import BaseBackend


class Backend(BaseBackend):
    """Pandas-backed timeseries data storage."""

    def __init__(self, storage, options=None):
        super().__init__(storage, options=options if options is not None else {})

    @staticmethod
    def _clean_dict(d):
        """Cleans dictionary of extraneous keywords."""
        remove_keys = ["_expires"]
        return {k: v for k, v in d.items() if k not in remove_keys}

    def _fs(self, name=None):
        fs, fs_token, paths = fsspec.get_fs_token_paths(
            self.storage,
            storage_options=self._clean_dict(self.options),
        )
        if name:
            feature_path = posixpath.join(paths[0], "feature", name)
        else:
            feature_path = posixpath.join(paths[0], "feature")
        return fs, feature_path

    def _full_feature_path(self, name):
        return posixpath.join(str(self.storage), "feature", name)

    def _list_partitions(self, name, n=None, reverse=False):
        """List the available partitions for a feature."""
        fs, feature_path = self._fs(name)
        try:
            objects = fs.ls(feature_path)
        except FileNotFoundError:
            return []
        
        partitions = []
        for _obj in objects:
            obj = _obj.replace(feature_path + os.path.sep, '')
            if obj.startswith("partition="):
                partitions.append(obj.split("=")[1])

        # partitions = [obj for obj in objects if obj.startswith("partition=")]
        # partitions = [p.split("=")[1] for p in partitions]
        partitions = sorted(partitions, reverse=reverse)
        if n:
            partitions = partitions[:n]
        return partitions

    @staticmethod
    def _apply_partition(partition, dt, offset=0):
        if isinstance(dt, dd.core.Series):
            if partition == "year":
                return dt.dt.year + offset
            elif partition == "date":
                return (dt + pd.Timedelta(days=offset)).dt.strftime("%Y-%m-%d")
            else:
                raise NotImplementedError(f"{partition} has not been implemented")

    def _write(self, name, ddf, **kwargs):
        # Write to output location
        feature_path = self._full_feature_path(name)
        # Build schema
        schema = {"time": pa.timestamp("ns"), "created_time": pa.timestamp("ns")}
        for field in pa.Table.from_pandas(ddf.head()).schema:
            if field.name in ["value", "partition"]:
                schema[field.name] = field.type
        try:
            ddf.to_parquet(
                feature_path,
                engine="pyarrow",
                compression="snappy",
                write_index=True,
                append=kwargs.get("append", False),
                partition_on="partition",
                ignore_divisions=True,
                schema=schema,
                storage_options=self._clean_dict(self.options),
            )
        except Exception as e:
            raise RuntimeError(f"Unable to save data to {feature_path}: {str(e)}")

    def _read(self, name, from_date=None, to_date=None, freq=None, time_travel=None, exactly_date=None, **kwargs):
        # Identify which partitions to read
        filters = []
        if exactly_date:
            filters.append(("time", "==", pd.Timestamp(exactly_date)))
        else:
            if from_date:
                filters.append(("time", ">=", pd.Timestamp(from_date)))
            if to_date:
                filters.append(("time", "<=", pd.Timestamp(to_date)))
        if kwargs.get("partitions"):
            for p in kwargs.get("partitions"):
                filters.append(("partition", "==", p))
        filters = filters if filters else None
        # Read the data
        feature_path = self._full_feature_path(name)
        try:
            ddf = dd.read_parquet(
                feature_path,
                engine="pyarrow",
                filters=filters,
                storage_options=self._clean_dict(self.options),
            )
            ddf = ddf.repartition(partition_size="25MB")
        except PermissionError as e:
            raise e
        # except Exception:
        #     # No data available
        #     empty_df = pd.DataFrame(columns=["time", "created_time", "value", "partition"]).set_index("time")
        #     ddf = dd.from_pandas(empty_df, chunksize=1)
        if "partition" in ddf.columns:
            ddf = ddf.drop(columns="partition")
        # Apply time-travel
        if time_travel:
            ddf = ddf.reset_index()
            ddf = ddf[ddf.created_time <= ddf.time + pd.Timedelta(time_travel)]
            ddf = ddf.set_index("time")
        # De-serialize from JSON if required
        if kwargs.get("serialized"):
            ddf = ddf.map_partitions(
                lambda df: df.assign(value=df.value.apply(pd.io.json.loads)),
                meta={
                    "value": "object",
                    "created_time": "datetime64[ns]",
                },
            )
        return ddf

    def ls(self):
        fs, path = self._fs()
        feature_names = [p.split("/")[-1] for p in fs.ls(path)]
        return feature_names

    def load(self, name, from_date=None, to_date=None, freq=None, time_travel=None, **kwargs):
        ddf = self._read(name, from_date, to_date, freq, time_travel, **kwargs)
        
        if not from_date:
            from_date = ddf.index.min().compute()  # First value in data
        if not to_date:
            to_date = ddf.index.max().compute()  # Last value in data
        if pd.Timestamp(to_date) < pd.Timestamp(from_date):
            to_date = from_date

        pdf = ddf.compute()
        
        # Keep only last created_time for each index timestamp
        pdf = pdf.reset_index().set_index("created_time").sort_index().groupby("time").last()

        # Apply resampling/date filtering
        if freq:
            samples = pd.DataFrame(index=pd.date_range(from_date, to_date, freq=freq))
            pdf = pd.merge(
                pd.merge(pdf, samples, left_index=True, right_index=True, how="outer").ffill(),
                samples,
                left_index=True,
                right_index=True,
                how="right",
            )
        else:
            # Filter on date range
            pdf = pdf.loc[pd.Timestamp(from_date) : pd.Timestamp(to_date)]  # noqa: E203

        return pdf

    # def _range(self, name, **kwargs):
    #     partitions = self._list_partitions(name)
    #     ddf = self._read(name, **kwargs)
        
    #     # Don't warn when querying empty feature
    #     with warnings.catch_warnings():
    #         warnings.simplefilter("ignore")
    #         first = ddf.head(1)
    #         last = ddf.tail(1)
        
    #     first = (
    #         {"time": None, "value": None} if first.empty else {"time": first.index[0], "value": first["value"].iloc[0]}
    #     )
        
    #     last = {"time": None, "value": None} if last.empty else {"time": last.index[0], "value": last["value"].iloc[0]}
    #     return first, last

    def first(self, name, from_date=None):
        partitions = self._list_partitions(name)
        ps = pd.to_datetime(partitions).sort_values()

        if from_date:
            ps = ps[ps >= pd.Timestamp(from_date)]

        first = ps.head(1)
        if first.empty:
            return None
        
        ddf = self._read(name, exactly_date=first)
        return ddf["value"]

    def last(self, name, to_date=None):
        partitions = self._list_partitions(name)
        ps = pd.to_datetime(partitions).sort_values()

        if to_date:
            ps = ps[ps <= pd.Timestamp(to_date)]

        last = ps.tail(1)
        if last.empty:
            return None
        
        ddf = self._read(name, exactly_date=last)
        return ddf["value"]

    def save(self, name, df, **kwargs):
        if df.empty:
            # Nothing to do
            return
        # Convert Pandas -> Dask
        if isinstance(df, pd.DataFrame):
            ddf = dd.from_pandas(df, chunksize=100000)
        elif isinstance(df, dd.DataFrame):
            ddf = df
        else:
            raise ValueError("Data must be supplied as a Pandas or Dask DataFrame")
        # Check value columm
        if "value" not in ddf.columns:
            raise ValueError("DataFrame must contain a value column")
        # Check we have a timestamp index column
        if np.issubdtype(ddf.index.dtype, np.datetime64):
            ddf = ddf.reset_index()
            if "time" in df.columns:
                raise ValueError("Not sure whether to use timestamp index or time column")
        # Check time column
        partition = kwargs.get("partition", "date")
        if "time" in ddf.columns:
            ddf = ddf.assign(time=ddf.time.astype("datetime64[ns]"))
            # Add partition column
            ddf = ddf.assign(partition=self._apply_partition(partition, ddf.time))
            ddf = ddf.set_index("time")
        else:
            raise ValueError(f"DataFrame must be supplied with timestamps, not {ddf.index.dtype}")
        # Check for created_time column
        if "created_time" not in ddf.columns:
            ddf = ddf.assign(created_time=pd.Timestamp.now())
        else:
            ddf = ddf.assign(created_time=ddf.created_time.astype("datetime64[ns]"))
        # Check for extraneous columns
        extraneous = set(ddf.columns) - set(["created_time", "value", "partition"])
        if len(extraneous) > 0:
            raise ValueError(f"DataFrame contains extraneous columns: {extraneous}")
        # Serialize to JSON if required
        if kwargs.get("serialized"):
            ddf = ddf.map_partitions(lambda df: df.assign(value=df.value.apply(pd.io.json.dumps)))
        # Save
        self._write(name, ddf, append=True)

    def delete(self, name):
        fs, feature_path = self._fs(name)
        try:
            fs.rm(feature_path, recursive=True)
        except FileNotFoundError:
            pass

    def _export(self, name):
        # Read the data
        feature_path = self._full_feature_path(name)
        try:
            ddf = dd.read_parquet(
                feature_path,
                engine="pyarrow",
                storage_options=self._clean_dict(self.storage_options),
            )
            # Repartition to optimise files on exported dataset
            ddf = ddf.repartition(partition_size="25MB")
            return ddf
        except Exception:
            # No data available
            return None

    def _import(self, name, ddf):
        if ddf is None or len(ddf.columns) == 0:
            return
        if "partition" not in ddf.columns:
            raise RuntimeError("Dask storage requires partitioning")
        # Copy data to new location
        self._write(name, ddf, append=False)
