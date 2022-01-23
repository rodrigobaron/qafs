import qafs
import pandas as pd
import pdb


fs = qafs.FeatureStore(
    connection_string='sqlite:///test.sqlite',
    url='/tmp/featurestore/tutorial'
)

fs.create_namespace(
    'tutorial', description='Tutorial datasets'
)

fs.create_feature('tutorial/numbers', description='Timeseries of numbers')
dts = pd.date_range('2020-01-01', '2021-02-09')
df = pd.DataFrame({'time': dts, 'value2': list(range(len(dts))), 'numbers': list(range(len(dts)))})

fs.save_dataframe(df, 'numbers', namespace='tutorial')

@fs.transform('tutorial/squared', from_features=['tutorial/numbers'])
def squared_numbers(df):
    pdb.set_trace()
    return df ** 2 


df_query = fs.load_dataframe(
    ['tutorial/numbers', 'tutorial/squared'],
    from_date='2021-01-01', to_date='2021-01-31'
)
pdb.set_trace()