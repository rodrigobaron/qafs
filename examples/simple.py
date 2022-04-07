import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema, io

import qafs
from qafs import InFeature, OutFeature

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
        OutFeature(name='numbers', namespace='example'),
        # OutFeature(name='squared', namespace='example')
    ],
    from_date='2021-01-01',
    to_date='2021-01-31'
)
print(df_query.shape)