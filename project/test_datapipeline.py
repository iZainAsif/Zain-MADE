import unittest
import pandas as pd
from pipeline import (
    datasource1,
    datasource2,
    merge_ds,
    store_data,
)

class TestPipeline(unittest.TestCase):
    def test_datasource1(self):
        state_climate = datasource1()
        self.assertIsInstance(state_climate, pd.DataFrame)
        # Add more specific assertions for the returned DataFrame if needed

    def test_datasource2(self):
        state_population = datasource2()
        self.assertIsInstance(state_population, pd.DataFrame)
        # Add more specific assertions for the returned DataFrame if needed

    def test_merge_ds(self):
        # Create sample input DataFrames for testing
        state_climate = pd.DataFrame({
            'State': ['State1', 'State2'],
            'year': [2010, 2011],
            'tempc': [25.5, 30.0],
        })
        state_population = pd.DataFrame({
            'State': ['State1', 'State2'],
            '2010': [1000000, 1500000],
        })
        merged_data = merge_ds(state_climate, state_population)
        self.assertIsInstance(merged_data, pd.DataFrame)
        # Add more specific assertions for the merged DataFrame if needed

    def test_store_data(self):
        # Create a sample DataFrame for testing
        df = pd.DataFrame({
            'State': ['State1', 'State2'],
            'value': [25.5, 30.0],
        })
        table_name = 'test_table'
        store_data(df, table_name)
        # Add assertions to check if data is stored properly in SQLite database
        # and verify if the table exists and has the expected data

    # Add more tests for other functions if needed

if __name__ == '__main__':
    unittest.main()
