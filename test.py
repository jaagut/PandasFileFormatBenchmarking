from typing import Dict
import os
import abc
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

class FormatBenchmarkTool:
    def __init__(self, 
            df: pd.DataFrame = df,
            write_dir: str = '.cache/'):
        """Initialize FormatBenchmarkTool.

        :param df: Pandas' dataframe to write
        :type df: pd.DataFrame, optional
        :param write_dir: Directory where to store write benchmarks, defaults to '.cache/'
        :type write_dir: str, optional
        """
        self.df = df
        self.write_dir = write_dir
        self.results : Dict|None = None

        # Create directory for writing, if necessary
        os.makedirs(self.write_dir, exist_ok=True)

    def run(self):
        """Run all benchmarks and collect results.
        """
        with (
            CSVBenchmark(self.df, os.path.join(self.write_dir, f'{df_size}.csv')) as csv_benchmark,
            ORCBenchmark(self.df, os.path.join(self.write_dir, f'{df_size}.orc')) as orc_benchmark,
            ParquetBenchmark(self.df, os.path.join(self.write_dir, f'{df_size}.parquet')) as parquet_benchmark,
            PickleBenchmark(self.df, os.path.join(self.write_dir, f'{df_size}.pkl')) as pickle_benchmark,
        ):
            self.results : Dict[AbstractBenchmark] = {
                'csv': csv_benchmark,
                'orc': orc_benchmark,
                'parquet': parquet_benchmark,
                'pickle': pickle_benchmark,
            }

    def get_results(self) -> Dict:
        """Returns the collected benchmark results.

        :return: Results of all benchmarks.
        :rtype: Dict
        """
        if self.results is None:
            self.run()
        return self.results

    def print_results(self):
        """Prints results of all benchmarks to stdout.
        """
        print(self.get_results())


class AbstractBenchmark:
    """Abstract Benchmark to be implemented for various file formats.
    This can be used as a context manager (with ...).
    """
    def __init__(self, df: pd.DataFrame, path: str):
        """Initialize Benchmark.
        A Benchmark implements dataframe write and read operations for a file format.

        :param df: The pandas' dataframe to write
        :type df: pd.DataFrame
        :param path: The path to write the benchmark to.
        :type path: str
        """
        self._df = df
        self._path : str = path
        self._results : Dict|None = None

    def collect_results(self, number_of_runs: int = 3):
        """Runs benchmarks and collects results

        :param number_of_runs: Number of repeated runs for benchmarks, defaults to 3
        :type number_of_runs: int, optional
        """
        self._results : Dict[float|None, float|None] = {
            'write_time' : None,
            'read_time' : None,
        }
        self._results['write_time'] = timeit.Timer(self.write).timeit(number=number_of_runs)
        # TODO: read

    def get_results(self) -> Dict[float|None, float|None]:
        """Returns the collected benchmark results.

        :return: Dict of results
        :rtype: Dict[float|None, float|None]
        """
        if self._results is None:
            self.collect_results()
        return self._results

    @abc.abstractmethod
    def write(self):
        """Write initialized dataframe to file
        """
        ...

    @abc.abstractmethod
    def read(self):
        ...

    def clean_files(self):
        if os.path.exists(self._path):
            os.remove(self._path)
            print(f"Cleaned '{self._path}'.")
        else:
            print(f"Could not clean '{self._path}', as it does not exist")

    def __enter__(self) -> Dict[float|None, float|None]:
        self.collect_results()
        return self.get_results()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.clean_files()

class CSVBenchmark(AbstractBenchmark):
    """Benchmarks .csv files.
    """
    def write(self):
        self._df.to_csv(self._path)

    def read(self):
        ...


class ORCBenchmark(AbstractBenchmark):
    """Benchmarks .orc files.
    """
    def write(self):
        table = pa.Table.from_pandas(self._df, preserve_index=False)
        orc.write_table(table, self._path)

    def read(self):
        ...


class ParquetBenchmark(AbstractBenchmark):
    """Benchmarks .parquet files.
    """
    def write(self):
        self._df.to_parquet(self._path)

    def read(self):
        ...


class PickleBenchmark(AbstractBenchmark):
    """Benchmarks .pkl (Pickle) files.
    """
    def write(self):
        with open(self._path, 'wb') as f:
            pickle.dump(self._df, f)

    def read(self):
        ...