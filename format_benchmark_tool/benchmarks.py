import re
from typing import Dict, List
import os
import abc
import timeit
import pandas as pd
import pyarrow
import pyarrow.orc as orc


def get_result_columns() -> List[str]:
    """Returns list of column names for dataframes collecting results

    :return: List of column names
    :rtype: List[str]
    """
    return [
        'format',       # str
        'write_time',   # float
        'file_size',    # int
        'read_time',    # float
        ]

class AbstractBenchmark:
    """Abstract Benchmark to be implemented for various file formats.
    This can be used as a context manager (with ...).
    """
    def __init__(self, test_data: pd.DataFrame, path: str, number_of_repeats: int):
        """Initialize Benchmark.
        A Benchmark implements dataframe write and read operations for a file format.

        :param test_data: The pandas' dataframe to write
        :type test_data: pd.DataFrame
        :param path: The path to write the benchmark to.
        :type path: str
        :param number_of_repeats: Number of repeated benchmark runs
        :type number_of_repeats: int
        """
        self.test_data = test_data
        self.path : str = path
        self.N = number_of_repeats

        self.format_name : str = self.get_format_name()  # To be set by each implementation

        self.columns : List[str] = get_result_columns()
        self.results : pd.DataFrame = pd.DataFrame([], columns=self.columns)  # Empty DataFrame with named columns for each metric

    @abc.abstractmethod
    def get_format_name(self) -> str:
        """Returns name of the handled format, e.g. -> 'csv'.

        :return: Format name
        :rtype: str
        """
        ...

    def collect_results(self):
        """Runs benchmarks and collects results
        """
        print(f"Running '{type(self).__name__}'..." + " "*25, end='\r')
        for _ in range(self.N):
            self.results = pd.concat([self.results, pd.DataFrame([[
                self.format_name,
                timeit.Timer(self.measure_write).timeit(number=1),  # Default "number" for each repeat is 1M!
                self.measure_file_size(),
                timeit.Timer(self.measure_read).timeit(number=1),  # Default "number" for each repeat is 1M!
            ]], columns=self.columns)], ignore_index=True)

    def get_results(self) -> pd.DataFrame:
        """Returns the collected benchmark results.

        :return: Benchmark results
        :rtype: pd.DataFrame
        """
        if self.results.empty:
            self.collect_results()
        return self.results

    @abc.abstractmethod
    def measure_write(self):
        """Write initialized dataframe to file
        """
        ...

    def measure_file_size(self) -> int:
        """Returns file size of previously written dataframe.

        :return: File size in bytes
        :rtype: int
        """
        return os.path.getsize(self.path)

    @abc.abstractmethod
    def measure_read(self):
        """Reads the previously written file.
        """
        ...

    def clean_files(self):
        if os.path.exists(self.path):
            os.remove(self.path)
            print(f"Cleaned '{self.path}'." + " "*25, end='\r')  # Overwrite console line
        else:
            print(f"Could not clean '{self.path}', as it does not exist")

    def __enter__(self) -> pd.DataFrame:
        """Runs benchmark and returns results.

        :return: Benchmark results
        :rtype: pd.DataFrame
        """
        self.collect_results()
        return self.get_results()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.clean_files()

class CSVBenchmark(AbstractBenchmark):
    """Benchmarks .csv files.
    """
    def get_format_name(self) -> str:
        return 'csv'

    def measure_write(self):
        self.test_data.to_csv(self.path)

    def measure_read(self):
        pd.read_csv(self.path)


class JSONBenchmark(AbstractBenchmark):
    """Benchmarks .json files.
    """
    def get_format_name(self) -> str:
        return 'json'

    def measure_write(self):
        self.test_data.to_json(self.path)

    def measure_read(self):
        pd.read_json(self.path)


class XMLBenchmark(AbstractBenchmark):
    """Benchmarks .xml files.
    """
    def get_format_name(self) -> str:
        return 'xml'

    def measure_write(self):
        self.test_data.to_xml(self.path)

    def measure_read(self):
        pd.read_xml(self.path)


class ExcelBenchmark(AbstractBenchmark):
    """Benchmarks .xlsx (Excel) files.
    """
    def get_format_name(self) -> str:
        return 'excel'

    def measure_write(self):
        self.test_data.to_excel(self.path)

    def measure_read(self):
        pd.read_excel(self.path)


class PickleBenchmark(AbstractBenchmark):
    """Benchmarks .pkl (Pickle) files.
    """
    def get_format_name(self) -> str:
        return 'pickle'

    def measure_write(self):
        self.test_data.to_pickle(self.path)

    def measure_read(self):
        pd.read_pickle(self.path)


class HDF5Benchmark(AbstractBenchmark):
    """Benchmarks .h5 (HDF5) files.
    """
    def get_format_name(self) -> str:
        return 'hdf5'

    def measure_write(self):
        self.test_data.to_hdf(self.path, 'table')

    def measure_read(self):
        pd.read_hdf(self.path, 'table')


class FeatherBenchmark(AbstractBenchmark):
    """Benchmarks .feather files.
    """
    def get_format_name(self) -> str:
        return 'feather'

    def measure_write(self):
        self.test_data.to_feather(self.path)

    def measure_read(self):
        pd.read_feather(self.path)


class ParquetBenchmark(AbstractBenchmark):
    """Benchmarks .parquet files.
    """
    def get_format_name(self) -> str:
        return 'parquet'

    def measure_write(self):
        self.test_data.to_parquet(self.path)

    def measure_read(self):
        pd.read_parquet(self.path)


class ORCBenchmark(AbstractBenchmark):
    """Benchmarks .orc files.
    """
    def get_format_name(self) -> str:
        return 'orc'

    def measure_write(self):
        # self._df.to_orc(self._path)  # Not implemented/compatible
        table = pyarrow.Table.from_pandas(self.test_data, preserve_index=False)
        orc.write_table(table, self.path)

    def measure_read(self):
        if os.name in ['posix']:
            pd.read_orc(self.path)  # Not yet supported on Windows...
        else:
            print("Falling back to manually reading ORC using pyarrow on Windows...")
            table = orc.read_table(self.path)
            pyarrow.Table.to_pandas(table)


class StataBenchmark(AbstractBenchmark):
    """Benchmarks .dta (Stata) files.
    """
    def get_format_name(self) -> str:
        return 'stata'

    def measure_write(self):
        self.test_data.to_stata(self.path)

    def measure_read(self):
        pd.read_stata(self.path)
