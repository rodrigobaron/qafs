import pandas as pd

from .fugue import Store as Fugue
from pyspark.sql import SparkSession
from fugue_spark import SparkExecutionEngine



class Store(Fugue):
    """Pandas-backed timeseries data storage (uses Dask for parquet functions)."""

    def __init__(self, url, storage_options={}):
        
        super().__init__(url, storage_options=storage_options)

    def _create_engine(self):
        spark_session = (SparkSession
                        .builder
                        .config("spark.executor.cores", 4)
                        .getOrCreate())
        return SparkExecutionEngine(spark_session)
    
    def _to_pandas(self, df):
        return df.toPandas()
