from typing import Dict
import os
import pandas as pd

from .benchmarks import *

# TODO: Test File Compression


class FormatBenchmarkTool:
    def __init__(self, 
            df: pd.DataFrame,
            number_of_repeats: int = 3,
            write_dir: str = '.cache/',
            file_prefix: str = 'benchmark'):
        """Initialize FormatBenchmarkTool.

        :param df: Pandas' dataframe to write
        :type df: pd.DataFrame, optional
        :param number_of_repeats: Number of repeated benchmark runs, defaults to 3
        :type number_of_repeats: int, optional
        :param write_dir: Directory where to store write benchmarks, defaults to '.cache/'
        :type write_dir: str, optional
        :param file_prefix: Prefix of written files' basename (file extension will be added automatically), defaults to 'benchmark'
        :type file_prefix: str, optional
        """
        self.test_data = df
        self.N = number_of_repeats
        self.write_dir = write_dir
        self.file_prefix = file_prefix


        self.columns : List[str] = get_result_columns()
        self.results : pd.DataFrame = pd.DataFrame([], columns=self.columns)  # Empty DataFrame with named columns for each metric

        # Create directory for writing, if necessary
        os.makedirs(self.write_dir, exist_ok=True)

    def run(self):
        """Run all benchmarks and collect results.
        """
        with (
            CSVBenchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.csv'), self.N) as csv_benchmark,
            JSONBenchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.json'), self.N) as json_benchmark,
            XMLBenchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.xml'), self.N) as xml_benchmark,
            ExcelBenchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.xlsx'), self.N) as excel_benchmark,
            PickleBenchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.pkl'), self.N) as pickle_benchmark,
            HDF5Benchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.h5'), self.N) as hdf5_benchmark,
            FeatherBenchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.feather'), self.N) as feather_benchmark,
            ParquetBenchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.parquet'), self.N) as parquet_benchmark,
            ORCBenchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.orc'), self.N) as orc_benchmark,
            StataBenchmark(self.test_data, os.path.join(self.write_dir, f'{self.file_prefix}.dta'), self.N) as stata_benchmark,
        ):
            self.results = pd.concat([
                self.results,
                csv_benchmark,
                json_benchmark,
                xml_benchmark,
                excel_benchmark,
                pickle_benchmark,
                hdf5_benchmark,
                feather_benchmark,
                parquet_benchmark,
                orc_benchmark,
                stata_benchmark,
            ], ignore_index=True)

    def get_results(self) -> Dict:
        """Returns the collected benchmark results.

        :return: Results of all benchmarks.
        :rtype: Dict
        """
        if self.results.empty:
            self.run()
        return self.results
