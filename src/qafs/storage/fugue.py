import posixpath
import warnings

import fsspec
import numpy as np
import pandas as pd
import pyarrow as pa
from fugue import FugueWorkflow, PartitionSpec, NativeExecutionEngine

from ._base import BaseStore


class Store(BaseStore):
    """Dask-backed timeseries data storage."""

    def __init__(self, url, storage_options=None):
        super().__init__(url, storage_options=storage_options if storage_options is not None else {})

    def _create_engine(self):
        return NativeExecutionEngine()
    
    def _to_pandas(self, df):
        # native have no op
        return df

    @staticmethod
    def _clean_dict(d):
        """Cleans dictionary of extraneous keywords."""
        remove_keys = ["_expires"]
        return {k: v for k, v in d.items() if k not in remove_keys}

    def _fs(self, name=None):
        fs, fs_token, paths = fsspec.get_fs_token_paths(
            self.url,
            storage_options=self._clean_dict(self.storage_options),
        )
        if name:
            feature_path = posixpath.join(paths[0], "feature", name)
        else:
            feature_path = posixpath.join(paths[0], "feature")
        return fs, feature_path

    def _full_feature_path(self, name):
        return posixpath.join(self.url, "feature", name)

    def _list_partitions(self, name, n=None, reverse=False):
        """List the available partitions for a feature."""
        fs, feature_path = self._fs(name)
        try:
            objects = fs.ls(feature_path)
        except FileNotFoundError:
            return []
        partitions = [obj for obj in objects if obj.startswith("partition=")]
        partitions = [p.split("=")[1] for p in partitions]
        partitions = sorted(partitions, reverse=reverse)
        if n:
            partitions = partitions[:n]
        return partitions

    @staticmethod
    def _apply_partition(partition, dt, offset=0):
        if isinstance(dt, pd.Series):
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
        # schema = {"time": pa.timestamp("ns"), "created_time": pa.timestamp("ns")}
        # for field in pa.Table.from_pandas(ddf.head()).schema:
        #     if field.name in ["value", "partition"]:
        #         schema[field.name] = field.type
        try:
            mode = "append" if kwargs.get("append", False) else "overwrite"
            ddf.save(
                feature_path,
                mode="overwrite",
                fmt="parquet",
                single=False,
                partition=PartitionSpec(by=["partition"]),
            )
            # ddf.to_parquet(
            #     feature_path,
            #     engine="pyarrow",
            #     compression="snappy",
            #     write_index=True,
            #     append=kwargs.get("append", False),
            #     partition_on="partition",
            #     ignore_divisions=True,
            #     schema=schema,
            #     storage_options=self._clean_dict(self.storage_options),
            # )
        except Exception as e:
            raise RuntimeError(f"Unable to save data to {feature_path}: {str(e)}")

    def _read(self, dag, name, from_date=None, to_date=None, freq=None, time_travel=None, **kwargs):
        # Identify which partitions to read
        # filters = []
        # if from_date:
        #     filters.append(("time", ">=", pd.Timestamp(from_date)))
        # if to_date:
        #     filters.append(("time", "<=", pd.Timestamp(to_date)))
        # if kwargs.get("partitions"):
        #     for p in kwargs.get("partitions"):
        #         filters.append(("partition", "==", p))
        # filters = filters if filters else None
        # Read the data
        feature_path = self._full_feature_path(name)
        try:
            ddf = dag.load(feature_path, fmt="parquet")
            # ddf = dd.read_parquet(
            #     feature_path,
            #     engine="pyarrow",
            #     filters=filters,
            #     storage_options=self._clean_dict(self.storage_options),
            # )
            # ddf = ddf.repartition(partition_size="25MB")
        except PermissionError as e:
            raise e
        except Exception:
            # No data available
            empty_df = pd.DataFrame(columns=["time", "created_time", "value", "partition"])  # .set_index("time")
            ddf = dag.df(df)
            # ddf = dd.from_pandas(empty_df, chunksize=1)

        def _clean_df(ddf: pd.DataFrame, time_travel, **kwargs) -> pd.DataFrame:
            if "partition" in ddf.columns:
                ddf = ddf.drop(columns="partition")
            # Apply time-travel
            if time_travel:
                ddf = ddf.reset_index()
                ddf = ddf[ddf.created_time <= ddf.time + pd.Timedelta(time_travel)]
                # ddf = ddf.set_index("time")
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

        ddf = ddf.transform(_clean_df, schema="*-partition", params={"time_travel": time_travel, "kwargs": kwargs})
        ddf.persist()
        return ddf

    def ls(self):
        fs, path = self._fs()
        feature_names = [p.split("/")[-1] for p in fs.ls(path)]
        return feature_names

    def load(self, name, from_date=None, to_date=None, freq=None, time_travel=None, **kwargs):
        def _load(ddf: pd.DataFrame, from_date, to_date, freq) -> pd.DataFrame:
            if not from_date:
                from_date = ddf['time'].min()  # .compute()  # First value in data
            if not to_date:
                to_date = ddf['time'].max()  # .compute()  # Last value in data
            if pd.Timestamp(to_date) < pd.Timestamp(from_date):
                to_date = from_date
            # pdf = ddf.compute()
            pdf = ddf
            # Keep only last created_time for each index timestamp
            # pdf = pdf.reset_index().set_index("created_time").sort_index().groupby("time").last()
            pdf = pdf.sort_values("created_time").groupby("time").last()
            # Apply resampling/date filtering
            if freq:
                samples = pd.DataFrame({'created_time': pd.date_range(from_date, to_date, freq=freq)})
                pdf = pd.merge(
                    pd.merge(pdf, samples, left_on="created_time", right_on="created_time", how="outer").ffill(),
                    samples,
                    # left_index=True,
                    # right_index=True,
                    left_on="created_time",
                    right_on="created_time",
                    how="right",
                )
            else:
                # Filter on date range
                pdf = pdf.loc[pd.Timestamp(from_date) : pd.Timestamp(to_date)]  # noqa: E203

            pdf = pdf.sort_values(["time", "created_time"])
            return pdf.reset_index()

        with FugueWorkflow(self._create_engine()) as dag:
            # Find the last value _before_ time range to carry over
            last_before = from_date
            if from_date:
                _, last_before = self._range(dag, name, to_date=from_date, time_travel=time_travel)
                last_before = last_before["time"]
            ddf = self._read(dag, name, last_before, to_date, freq, time_travel, **kwargs)

            ddf = ddf.transform(_load, schema="*", params={"from_date": from_date, "to_date": to_date, "freq": freq})
            ddf.yield_dataframe_as("df")
            ddf = dag.run()['df'].native

        return self._to_pandas(ddf)

    def _range(self, dag, name, **kwargs):
        ddf = self._read(dag, name, **kwargs)

        # Don't warn when querying empty feature
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            ddf.yield_dataframe_as("df")
            df = dag.run()["df"].native
            
            first = df.head(1)
            last = df.tail(1)

            first_is_empty = len(first) == 0
            last_is_empty = len(last) == 0
            
            first_obj, last_obj = None, None

            if not first_is_empty:
                if isinstance(first, list):
                    first_obj = first[0]
                else:
                    first_obj = first.iloc[0]
            
            if not last_is_empty:
                if isinstance(last, list):
                    last_obj = last[0]
                else:
                    last_obj = last.iloc[0]
            
        first = (
            {"time": None, "value": None} if first_is_empty else {"time": first_obj["time"], "value": first_obj["value"]}
        )
        last = {"time": None, "value": None} if last_is_empty else {"time": last_obj["time"], "value": last_obj["value"]}
        return first, last

    def first(self, name, **kwargs):
        first, _ = self._range(name, **kwargs)
        return first["value"]

    def last(self, name, **kwargs):
        _, last = self._range(name, **kwargs)
        return last["value"]

    def save(self, name, df, **kwargs):
        if df.empty:
            # Nothing to do
            return
        # Convert Pandas -> Dask
        # if isinstance(df, pd.DataFrame):
        #     ddf = dd.from_pandas(df, chunksize=100000)
        # elif isinstance(df, dd.DataFrame):
        #     ddf = df
        # else:
        #     raise ValueError("Data must be supplied as a Pandas or Dask DataFrame")

        def _save(ddf: pd.DataFrame, **kwargs) -> pd.DataFrame:
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
                # ddf = ddf.set_index("time")
            else:
                raise ValueError(f"DataFrame must be supplied with timestamps, not {ddf.index.dtype}")
            # Check for created_time column
            if "created_time" not in ddf.columns:
                ddf = ddf.assign(created_time=pd.Timestamp.now())
            else:
                ddf = ddf.assign(created_time=ddf.created_time.astype("datetime64[ns]"))
            # Check for extraneous columns
            extraneous = set(ddf.columns) - set(["created_time", "value", "partition"])
            # if len(extraneous) > 0:
            #     raise ValueError(f"DataFrame contains extraneous columns: {extraneous}")
            # Serialize to JSON if required
            if kwargs.get("serialized"):
                ddf = ddf.map_partitions(lambda df: df.assign(value=df.value.apply(pd.io.json.dumps)))
            return ddf

        with FugueWorkflow(self._create_engine()) as dag:
            ddf = dag.df(df).transform(
                _save, params={'kwargs': kwargs}, schema="*, partition:datetime,created_time:datetime"
            )
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
