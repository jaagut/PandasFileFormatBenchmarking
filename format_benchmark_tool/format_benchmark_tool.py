from typing import Dict
import os
import numpy as np
import pandas as pd

from benchmarks import *

# TODO: Test File Compression

# How often to repeat benchmark runs
NUMBER_OF_RUNS : int = 3

# Generate data
np.random.seed = 2908
DF_SIZE = 1000_000
DF = pd.DataFrame({
    'a': np.random.rand(DF_SIZE),
    'b': np.random.rand(DF_SIZE),
    'c': np.random.rand(DF_SIZE),
    'd': np.random.rand(DF_SIZE),
    'e': np.random.rand(DF_SIZE)
})

class FormatBenchmarkTool:
    def __init__(self, 
            df: pd.DataFrame = DF,
            write_dir: str = '.cache/'):
        """Initialize FormatBenchmarkTool.

        :param df: Pandas' dataframe to write, defaults to DF
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
            CSVBenchmark(self.df, os.path.join(self.write_dir, f'{DF_SIZE}.csv')) as csv_benchmark,
            JSONBenchmark(self.df, os.path.join(self.write_dir, f'{DF_SIZE}.json')) as json_benchmark,
            XMLBenchmark(self.df, os.path.join(self.write_dir, f'{DF_SIZE}.xml')) as xml_benchmark,
            PickleBenchmark(self.df, os.path.join(self.write_dir, f'{DF_SIZE}.pkl')) as pickle_benchmark,
            HDF5Benchmark(self.df, os.path.join(self.write_dir, f'{DF_SIZE}.h5')) as hdf5_benchmark,
            FeatherBenchmark(self.df, os.path.join(self.write_dir, f'{DF_SIZE}.feather')) as feather_benchmark,
            ParquetBenchmark(self.df, os.path.join(self.write_dir, f'{DF_SIZE}.parquet')) as parquet_benchmark,
            ORCBenchmark(self.df, os.path.join(self.write_dir, f'{DF_SIZE}.orc')) as orc_benchmark,
            StataBenchmark(self.df, os.path.join(self.write_dir, f'{DF_SIZE}.dta')) as stata_benchmark,
        ):
            self.results : Dict[AbstractBenchmark] = {
                'csv': csv_benchmark,
                'json': json_benchmark,
                'xml': xml_benchmark,
                'pickle': pickle_benchmark,
                'hdf5': hdf5_benchmark,
                'feather': feather_benchmark,
                'parquet': parquet_benchmark,
                'orc': orc_benchmark,
                'stata': stata_benchmark,
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
