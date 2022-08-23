import pandas as pd
import numpy as np
import requests
import logging

dataset = '100073'
date_column = 'timestamp'
columns_dataset = ['ndiff_conf', 'ndiff_deceased']
str_column = ','.join(columns_dataset)

DATA_URL = f"https://data.bs.ch/api/v2/catalog/datasets/{dataset}/exports/" \
            f"json?order_by={date_column}&select={date_column}&select={str_column}" \
            "&timezone=Europe%2FBerlin"

# DATA_URL = "https://data.bs.ch/api/v2/catalog/datasets/100073/exports/" \
#            "json?order_by=timestamp&select=timestamp&select=ndiff_conf&select=ndiff_released&select=" \
#            "ndiff_deceased&timezone=Europe%2FBerlin"


class Fallzahlen:

    def __init__(self, data_url=None):
        self.data_url = data_url
        self.data: pd.DataFrame = None
        self.load_data()
        self.data_structure = {}
        self.data_detected = []
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
        fill NaN's according to method
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
            df = self.data[column].iloc[:-1]
            max_diffs[column] = df.diff().max()
        self.data_structure['max_diffs'] = max_diffs

    def largest_percentage(self, column=None):
        columns = [column] if column is not None else self.data_structure['column_names'][1:]
        max_percs = {}
        for column in columns:
            df = self.data[column].iloc[:-1]
            df_perc = df.pct_change()
            df_perc.replace([np.inf, -np.inf], np.nan, inplace=True)
            max_percs[column] = df_perc.max()
        self.data_structure['max_percs'] = max_percs

    def check_one_entry_per_day(self):
        # date/timestamp is always first column
        df = self.data.iloc[:, 0]
        days = [x.day for x in pd.to_datetime(df)]
        days[-1] == days[-2]
        self.data_detected.append('There is more than one entry for the last entered day.')

    def check_if_negative_entry(self, column=None):
        columns = [column] if column is not None else self.data_structure['column_names'][1:]
        for column in columns:
            df = self.data[column]
            if df.iloc[-1] < 0:
                self.data_detected.append(f"There is a negative value in column {column}")

    def check_difference(self, column=None):
        self.largest_difference()
        columns = [column] if column is not None else self.data_structure['column_names'][1:]
        for column in columns:
            diff_with_previous_day = abs(self.data[column][-1] - self.data[column][-2])
            max_ever = self.data_structure['max_diffs'][column]
            if diff_with_previous_day > max_ever:
                self.data_detected.append(f"In columnn {column}: Difference with previous day ({diff_with_previous_day}) larger than ever before ({max_ever}) ")

    def check_percentage(self, column=None):
        self.largest_percentage()
        columns = [column] if column is not None else self.data_structure['column_names'][1:]
        for column in columns:
            perc_with_previous_day = abs(self.data[column][-1] - self.data[column][-2])/self.data[column][-2]
            perc_ever = self.data_structure['perc_diffs'][column]
            if perc_with_previous_day > perc_ever:
                self.data_detected.append(f"In columnn {column}: Percentage difference with previous day ({perc_with_previous_day}) larger than ever before ({perc_ever}) ")

    def rolling_sum(self):
        # df.rolling(5).sum()
        pass

    def check_slope(self):
        pass

    def check_email(self):
        # note: we only want to send email once per day! make log file...
        if self.data_detected != []:
            logging.info(f"send email with info {self.data_detected}")
            subject = f"possible issues with dataset..."
            text = f"The automatic plausibilisation job has detected the following: {self.data_detected}"


if __name__ == "__main__":
    fz = Fallzahlen(data_url=DATA_URL)
    fz.fill_nan()
    print(fz.data)
    fz.largest_difference()
    fz.largest_percentage()
    print(fz.data_structure)
