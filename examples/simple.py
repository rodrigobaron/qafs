import qafs
import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from pandera import io


fs = qafs.FeatureStore(
    connection_string='sqlite:///test.sqlite',
    url='/tmp/featurestore/tutorial'
)

fs.create_namespace(
    'tutorial', description='Tutorial datasets'
)
fs.create_feature('tutorial/numbers', description='Timeseries of numbers', check=Column(pa.Int, Check.greater_than(1)))


dts = pd.date_range('2020-01-01', '2021-02-09')
df = pd.DataFrame({'time': dts, 'tutorial/numbers': list(range(1, len(dts)+1))})

fs.save_dataframe(df, 'tutorial/numbers')

@fs.transform('tutorial/squared', from_features=['tutorial/numbers'], check=Column(pa.Int, Check.greater_than(0)))
def squared_numbers(df):
    return df ** 2 


df_query = fs.load_dataframe(
    ['tutorial/numbers', 'tutorial/squared'],
    from_date='2021-01-01', to_date='2021-01-31'
)

import pdb; pdb.set_trace()
