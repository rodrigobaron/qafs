# Quality Aware Feature Store

<p align="center">
    <a href="https://github.com/rodrigobaron/qafs/actions/workflows/build.yaml">
        <img alt="Build" src="https://github.com/rodrigobaron/qafs/actions/workflows/build.yaml/badge.svg">
    </a>
    <a href="https://github.com/rodrigobaron/qafs/blob/main/LICENSE">
        <img alt="GitHub" src="https://img.shields.io/github/license/rodrigobaron/qafs.svg?color=blue">
    </a>
    <a href="https://github.com/rodrigobaron/qafs/releases">
        <img alt="GitHub release" src="https://img.shields.io/github/release/rodrigobaron/qafs.svg">
    </a>
</p>

<h3 align="center">
    Simple and scalable feature store with data quality checks.
</h3>

feature store aim to solve the data management problems when building Machine Learning applications. However the data quality is a component which data teams need integrate and handle as separated component. This project join both concepts keeping the data quality closely coupled with data transformations making necessary a minimal data verification check and possibiliting the data/transformations check evolve during the projects.

For that **qafs** have a strong dependecy with [pandera](https://pandera.readthedocs.io/) to build the data validations.


## Features

* Pandas-like API
* Features information stored in database along with metadata.
* Dask to process large datasets in a cluster enviroment.
* Data is stored as timeseries in [Parquet format](https://parquet.apache.org/), store in filesystem or object storage services.
* Store transformations as feature.


## Get Started

Installing the python package through pip:  

```bash
$ pip install qafs
```

Bellow is an example of usage **qafs** where we'll create a feature store and register `numbers` feature and an `squared` feature transformation. First we need import the packages and create the feature store, for this example we are using sqlite database and persisting the features in the filesystem:  

```python
import qafs
import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from pandera import io


fs = qafs.FeatureStore(
    connection_string='sqlite:///test.sqlite',
    url='/tmp/featurestore/example'
)
```

Features could be stored in namespaces, it help organize the data. When creating `numbers` we specify the `'example/numbers'` feature to point the feature `numbers`at that namespace `example` however we can use the arguments `name='numbers', namespace='example'` as well. The we specify the data validation using **pandera** telling that feature is `Integer` and the values should be `greater than 0`:

```python
fs.create_namespace('example', description='Example datasets')
fs.create_feature(
    'example/numbers',
    description='Timeseries of numbers',
    check=Column(pa.Int, Check.greater_than(0))
)


dts = pd.date_range('2020-01-01', '2021-02-09')
df = pd.DataFrame({'time': dts, 'numbers': list(range(1, len(dts) + 1))})

fs.save_dataframe(df, name='numbers', namespace='example')

```

To register our `squared` transformation feature we're using the annotation `fs.transform` and fetching the data from the `numbers` feature applying the same data validation from `numbers`:
```python
@fs.transform(
    'example/squared',
    from_features=['example/numbers'],
    check=Column(pa.Int, Check.greater_than(0))
)
def squared(df):
    return df ** 2

```

When fetch our features we should see:
```python
df_query = fs.load_dataframe(
    ['example/numbers', 'example/squared'], 
    from_date='2021-01-01',
    to_date='2021-01-31'
)
print(df_query.tail(1))
##----
#             example/numbers  example/squared
# time                                        
# 2021-01-31              397           157609
##----
```

## Contributing

Please follow the [Contributing](CONTRIBUTING.md) guide.

## License

[GPL-3.0 License](LICENSE)  

This project started using the as base [bytehub feature store](https://github.com/bytehub-ai/bytehub) and is under the same license.