import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema, io

import qafs

fs = qafs.FeatureStore(
    connection_string='sqlite:///test.sqlite', url='/tmp/featurestore/example', backend='fugue_spark'
)

fs.create_namespace('example', description='Example datasets')
fs.create_feature('example/numbers', description='Timeseries of numbers', check=Column(pa.Int, Check.greater_than(0)))


dts = pd.date_range('2020-01-01', '2021-02-09')
df = pd.DataFrame({'time': dts, 'numbers': list(range(1, len(dts) + 1))})
fs.save_dataframe(df, name='numbers', namespace='example')


@fs.transform('example/squared', from_features=['example/numbers'], check=Column(pa.Int, Check.greater_than(0)))
def squared(df):
    return df ** 2


df_query = fs.load_dataframe(['example/numbers', 'example/squared'], from_date='2021-01-01', to_date='2021-01-31')
print(df_query.tail(1))
