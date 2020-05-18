from covid19dashboard import credentials
import pandas as pd
import os
import common

sourcefile = 'https://raw.githubusercontent.com/openZH/covid_19/master/COVID19_Fallzahlen_CH_total_v2.csv'
print(f'Reading date from {sourcefile}...')
df = pd.read_csv(sourcefile)
print('Calculating date range...')
df.date = pd.to_datetime(df.date)
date_range = pd.date_range(start=df.date.min(), end=df.date.max())

# https://skipperkongen.dk/2018/11/26/how-to-fill-missing-dates-in-pandas/
# https://stackoverflow.com/questions/52430892/forward-filling-missing-dates-into-python-pandas-dataframe
print('Iterating over each canton, sorting, adding missing dates, then filling the value gaps using ffill()...')
cantons = df.abbreviation_canton_and_fl.unique()
df_filled = pd.DataFrame(columns=df.columns)
for canton in cantons:
    df_canton = df[df.abbreviation_canton_and_fl == canton].sort_values(by='date')
    df_canton_filled = df_canton.set_index('date').reindex(date_range).ffill().reset_index().rename(columns={'index': 'date'})
    df_filled = df_filled.append(df_canton_filled, ignore_index=True)

print('Getting rid of unnecessary columns and rows with empty date...')
df_filled.drop(columns=['time', 'source', 'ncumul_tested', 'new_hosp', 'current_vent'], inplace=True)
df_filled.dropna(subset=['abbreviation_canton_and_fl'], inplace=True)

filename = os.path.join(credentials.path, credentials.filename)
print(f'Exporting data to {filename}')
df_filled.to_csv(filename, index=False)

common.upload_ftp(filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'covid19dashboard')