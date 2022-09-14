import os
import timeit
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc 
import pickle
import feather
from fastavro import writer, reader, parse_schema

# TODOs: Implement feather, hdf5, stata?
# TODOs: Read benchmarks
# TODOs: Compression

# Directory where to store benchmark files
dir = '.cache/'
os.makedirs(dir, exist_ok=True)

formats = {
    'csv': {
        'path': dir + '10M.csv',
        'func': None,
        'write_time': None,
    },
    'orc': {
        'path': dir + '10M.orc',
        'func': None,
        'write_time': None,
    },
    'parquet': {
        'path': dir + '10M.parquet',
        'func': None,
        'write_time': None,
    },
    'pkl': {
        'path': dir + '10M.pkl',
        'func': None,
        'write_time': None,
    },
}

# Generate data
np.random.seed = 2908
df_size = 1000_000
df = pd.DataFrame({
    'a': np.random.rand(df_size),
    'b': np.random.rand(df_size),
    'c': np.random.rand(df_size),
    'd': np.random.rand(df_size),
    'e': np.random.rand(df_size)
})


def WRITE_CSV_fun_timeIt():
    df.to_csv(formats['csv']['path'])
formats['csv']['func'] = WRITE_CSV_fun_timeIt

def WRITE_ORC_fun_timeIt():
    table = pa.Table.from_pandas(df, preserve_index=False)
    orc.write_table(table, formats['orc']['path'])
formats['orc']['func'] = WRITE_ORC_fun_timeIt

def WRITE_PARQUET_fun_timeIt():
    df.to_parquet(formats['parquet']['path'])
formats['parquet']['func'] = WRITE_PARQUET_fun_timeIt

def WRITE_PICKLE_fun_timeIt():
    with open(formats['pkl']['path'], 'wb') as f:
        pickle.dump(df, f)
formats['pkl']['func'] = WRITE_PICKLE_fun_timeIt

def benchmark_writes(number_of_runs=3):
    for k, v in formats.items():
        print(f"Benchmarking format {k}...")
        time = timeit.Timer(v['func']).timeit(number=number_of_runs)
        v['write_time'] = time
    print_results()

def print_results():
    for k, v in formats.items():
        print(f"{k}: {v['write_time']} s")

def clean_files():
    for format in formats.values():
        path = format['path']
        if os.path.exists(path):
            os.remove(path)
            print(f"Removed '{path}'.")
        else:
            print(f"Could not remove '{path}', as it does not exist")
