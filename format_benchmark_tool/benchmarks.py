from typing import Dict, List
import os
import abc
import timeit
import pandas as pd
import pyarrow
import pyarrow.orc as orc


class AbstractBenchmark:
    """Abstract Benchmark to be implemented for various file formats.
    This can be used as a context manager (with ...).
    """
    def __init__(self, df: pd.DataFrame, path: str, number_of_repeats: int):
        """Initialize Benchmark.
        A Benchmark implements dataframe write and read operations for a file format.

        :param df: The pandas' dataframe to write
        :type df: pd.DataFrame
        :param path: The path to write the benchmark to.
        :type path: str
        :param number_of_repeats: Number of repeated benchmark runs
        :type number_of_repeats: int
        """
        self._df = df
        self._path : str = path
        self._number_of_repeats = number_of_repeats

        self._results : Dict|None = None

    def collect_results(self):
        """Runs benchmarks and collects results
        """
        print(f"Running '{type(self).__name__}'..." + " "*25, end='\r')
        self._results : Dict[str, List[float]|int|None] = {
            'write_time': None,
            'file_size': None,
            'read_time': None,
        }
        self._results['write_time'] = timeit.Timer(self.measure_write).repeat(repeat=self._number_of_repeats, number=1)  # Default "number" for each repeat is 1M!
        self._results['file_size'] = self.measure_file_size()
        self._results['read_time'] = timeit.Timer(self.measure_read).repeat(repeat=self._number_of_repeats, number=1)  # Default "number" for each repeat is 1M!

    def get_results(self) -> Dict[str, List[float]|int|None]:
        """Returns the collected benchmark results.

        :return: Dict of results
        :rtype: Dict[str, List[float]|int|None]
        """
        if self._results is None:
            self.collect_results()
        return self._results

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
        return os.path.getsize(self._path)

    @abc.abstractmethod
    def measure_read(self):
        """Reads the previously written file.
        """
        ...

    def clean_files(self):
        if os.path.exists(self._path):
            os.remove(self._path)
            print(f"Cleaned '{self._path}'." + " "*25, end='\r')  # Overwrite console line
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
    def measure_write(self):
        self._df.to_csv(self._path)

    def measure_read(self):
        pd.read_csv(self._path)


class JSONBenchmark(AbstractBenchmark):
    """Benchmarks .json files.
    """
    def measure_write(self):
        self._df.to_json(self._path)

    def measure_read(self):
        pd.read_json(self._path)


class XMLBenchmark(AbstractBenchmark):
    """Benchmarks .xml files.
    """
    def measure_write(self):
        self._df.to_xml(self._path)

    def measure_read(self):
        pd.read_xml(self._path)


class ExcelBenchmark(AbstractBenchmark):
    """Benchmarks .xlsx (Excel) files.
    """
    def measure_write(self):
        self._df.to_excel(self._path)

    def measure_read(self):
        pd.read_excel(self._path)


class PickleBenchmark(AbstractBenchmark):
    """Benchmarks .pkl (Pickle) files.
    """
    def measure_write(self):
        self._df.to_pickle(self._path)

    def measure_read(self):
        pd.read_pickle(self._path)


class HDF5Benchmark(AbstractBenchmark):
    """Benchmarks .h5 (HDF5) files.
    """
    def measure_write(self):
        self._df.to_hdf(self._path, 'table')

    def measure_read(self):
        pd.read_hdf(self._path, 'table')


class FeatherBenchmark(AbstractBenchmark):
    """Benchmarks .feather files.
    """
    def measure_write(self):
        self._df.to_feather(self._path)

    def measure_read(self):
        pd.read_feather(self._path)


class ParquetBenchmark(AbstractBenchmark):
    """Benchmarks .parquet files.
    """
    def measure_write(self):
        self._df.to_parquet(self._path)

    def measure_read(self):
        pd.read_parquet(self._path)


class ORCBenchmark(AbstractBenchmark):
    """Benchmarks .orc files.
    """
    def measure_write(self):
        # self._df.to_orc(self._path)  # Not implemented/compatible
        table = pyarrow.Table.from_pandas(self._df, preserve_index=False)
        orc.write_table(table, self._path)

    def measure_read(self):
        if os.name in ['posix']:
            pd.read_orc(self._path)  # Not yet supported on Windows...
        else:
            print("Falling back to manually reading ORC using pyarrow on Windows...")
            table = orc.read_table(self._path)
            pyarrow.Table.to_pandas(table)


class StataBenchmark(AbstractBenchmark):
    """Benchmarks .dta (Stata) files.
    """
    def measure_write(self):
        self._df.to_stata(self._path)

    def measure_read(self):
        pd.read_stata(self._path)
