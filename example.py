import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema, io

import qafs

# fs = qafs.FeatureStore(
#     connection_string='sqlite:///test.sqlite', url='/tmp/featurestore/example', backend='fugue_spark'
# )


fs = qafs.FeatureStore(
    db_connection='sqlite:///test.sqlite',
    storage=qafs.LocalStorage(path='/tmp/featurestore/example'),
    backend='pandas'
)

fs.create_namespace('example', description='Example datasets')
fs.create_feature(
    'numbers',
    namespace="example", 
    description='Timeseries of numbers', 
    check=Column(pa.Int, Check.greater_than(0))
)

dts = pd.date_range('2020-01-01', '2021-02-09')
df = pd.DataFrame({'time': dts, 'numbers': list(range(1, len(dts) + 1))})
# fs.save_dataframe(df, name='numbers', namespace='example')
# fs.ingest_from_csv('path/to/file.csv', name='numbers', namespace='example')
# fs.ingest_from_csv('path/to/file.parquet', name='numbers', namespace='example')
fs.save_df(df, name='numbers', namespace='example')


@fs.transform(
    name='squared', 
    namespace='example', 
    from_features=[
        InFeature(name='numbers', namespace='example')
    ], 
    check=Column(pa.Int, Check.greater_than(0))
)
def squared(df):
    return df ** 2


df_query = fs.load_features(
    features=[
        OutFeatures(name='numbers', namespace='example'),
        OutFeatures(name='squared', namespace='example')
    ],
    from_date='2021-01-01',
    to_date='2021-01-31'
)
print(df_query.tail(1))

fs.materialize(features=[
    OutFeatures(name='numbers', namespace='example'),
    OutFeatures(name='squared', namespace='example')
])
