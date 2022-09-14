from typing import Dict
import os
import pandas as pd

from benchmarks import *

# TODO: Test File Compression


class FormatBenchmarkTool:
    def __init__(self, 
            df: pd.DataFrame,
            number_of_runs: int = 3,
            write_dir: str = '.cache/',
            file_prefix: str = 'benchmark'):
        """Initialize FormatBenchmarkTool.

        :param df: Pandas' dataframe to write
        :type df: pd.DataFrame, optional
        :param number_of_runs: Number of repeated benchmark runs, defaults to 3
        :type number_of_runs: int, optional
        :param write_dir: Directory where to store write benchmarks, defaults to '.cache/'
        :type write_dir: str, optional
        :param file_prefix: Prefix of written files' basename (file extension will be added automatically), defaults to 'benchmark'
        :type file_prefix: str, optional
        """
        self.df = df
        self.number_of_runs = number_of_runs
        self.write_dir = write_dir
        self.file_prefix = file_prefix

        self.results : Dict|None = None

        # Create directory for writing, if necessary
        os.makedirs(self.write_dir, exist_ok=True)

    def run(self):
        """Run all benchmarks and collect results.
        """
        with (
            CSVBenchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.csv', self.number_of_runs)) as csv_benchmark,
            JSONBenchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.json', self.number_of_runs)) as json_benchmark,
            XMLBenchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.xml', self.number_of_runs)) as xml_benchmark,
            ExcelBenchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.xlsx', self.number_of_runs)) as excel_benchmark,
            PickleBenchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.pkl', self.number_of_runs)) as pickle_benchmark,
            HDF5Benchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.h5', self.number_of_runs)) as hdf5_benchmark,
            FeatherBenchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.feather', self.number_of_runs)) as feather_benchmark,
            ParquetBenchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.parquet', self.number_of_runs)) as parquet_benchmark,
            ORCBenchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.orc', self.number_of_runs)) as orc_benchmark,
            StataBenchmark(self.df, os.path.join(self.write_dir, f'{self.file_prefix}.dta', self.number_of_runs)) as stata_benchmark,
        ):
            self.results : Dict[AbstractBenchmark] = {
                'csv': csv_benchmark,
                'json': json_benchmark,
                'xml': xml_benchmark,
                'excel': excel_benchmark,
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
