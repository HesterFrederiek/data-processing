import pandas as pd
import requests


DATA_URL = "https://data.bs.ch/api/v2/catalog/datasets/100073/exports/" \
           "json?order_by=timestamp&select=timestamp&select=ndiff_conf&select=ndiff_released&select=" \
           "ndiff_deceased&timezone=Europe%2FBerlin"


class Fallzahlen:

    def __init__(self, data_url=None):
        self.data_url = data_url
        self.data: pd.DataFrame = None
        self.load_data()

        self.data_structure = {}
        self.analyze_data()

    def load_data(self):
        try:
            assert self.data_url is not None, "Data URL is not set, cannot load data..."

            req = requests.get(self.data_url)
            file = req.json()
            self.data = pd.DataFrame.from_dict(file)

        except AssertionError as e:
            print(e)

    def analyze_data(self):
        self.data_structure['column_names'] = list(self.data.columns)

    def fill_nan(self, column=None, method='ffill'):
        """
        Description what the function does...
        :param column: specific column name, if only to be executed on that column
        :param method: specifies fill method. One of ('ffill', 'method2')
        :return: None
        """
        columns = [column] if column is not None else self.data_structure['column_names']
        for column in columns:
            self.data[column].fillna(method='ffill', inplace=True)

    def largest_difference(self, column=None):
        columns = [column] if column is not None else self.data_structure['column_names'][1:]
        max_diffs = {}
        for column in columns:
            df = self.data[column]
            max_diffs[column] = df.diff().max()
        self.data_structure['max_diffs'] = max_diffs


if __name__ == "__main__":
    fz = Fallzahlen(data_url=DATA_URL)
    fz.fill_nan()
    print(fz.data)
    fz.largest_difference()
    print(fz.data_structure)