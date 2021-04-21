from datetime import datetime
import urllib3
import os
import pandas as pd
import common
import json
from requests.auth import HTTPBasicAuth
from functools import reduce
from bafu_hydrodaten import credentials

print('Loading data into data frames...')
dfs = []
for file in credentials.files:
    response = common.requests_get(f'{credentials.https_url}/{file}', auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass), stream=True)
    df = pd.read_csv(response.raw, parse_dates=True, infer_datetime_format=True)
    dfs.append(df)

print(f'Merging data frames...')
all_df = reduce(lambda left, right: pd.merge(left, right, on=['Time'], how='outer'), dfs)
all_filename = f"{os.path.join(credentials.path, 'bafu_hydrodaten/data/')}hydrodata_{datetime.today().strftime('%Y-%m-%d')}.csv"
all_df.to_csv(all_filename, index=False)
common.upload_ftp(all_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, credentials.ftp_dir_all)

print('Processing data...')
merged_df = all_df.copy(deep=True)
merged_df['timestamp'] = pd.to_datetime(merged_df.Time, infer_datetime_format=True)
# timestamp is a text column used tof pushing into ODS realtime API
merged_df['timestamp_text'] = merged_df.timestamp.dt.strftime('%Y-%m-%dT%H:%M:%S%z')
merged_df['datum'] = merged_df.timestamp.dt.strftime('%d.%m.%Y')
merged_df['zeit'] = merged_df.timestamp.dt.strftime('%H:%M')
merged_df['intervall'] = 5
merged_df['abfluss'] = merged_df['BAFU_2289_AbflussRadar']
merged_df['pegel'] = merged_df['BAFU_2289_PegelRadar']
# merged_df = merged_df[['datum', 'zeit', 'abfluss', 'intervall', 'pegel', 'timestamp_dt', 'timestamp']]
# drop rows if all cells are empty in certain columns
merged_df = merged_df.dropna(subset=['abfluss', 'pegel'], how='all')

local_path = os.path.join(credentials.path, 'bafu_hydrodaten/data')
merged_filename = os.path.join(local_path, f'2289_pegel_abfluss_{datetime.today().strftime("%Y-%m-%d")}.csv')
print(f'Exporting data to {merged_filename}...')
merged_df.to_csv(merged_filename, columns=['datum', 'zeit', 'abfluss', 'intervall', 'pegel', 'timestamp'], index=False)

common.upload_ftp(merged_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, credentials.ftp_remote_dir)

urllib3.disable_warnings()
# print(f'Retrieving latest record from ODS...')
# r = common.requests_get(url='https://data.bs.ch/api/records/1.0/search/?dataset=100089&q=&rows=1&sort=timestamp', verify=False)
# r.raise_for_status()
# latest_ods_value = r.json()['records'][0]['fields']['timestamp']

# print(f'Filtering data after {latest_ods_value} for submission to ODS via realtime API...')
# realtime_df = merged_df[merged_df['timestamp'] > latest_ods_value]
realtime_df = merged_df

if len(realtime_df) == 0:
    print(f'No rows to push to ODS... ')
else:
    # Realtime API bootstrap data:
    # {
    #   "timestamp": "2020-07-28T01:35:00+02:00",
    #   "pegel": "245.16",
    #   "abfluss": "591.2"
    # }

    # only keep columns that need to be pushed, and rename if necessary.
    realtime_df = realtime_df[['timestamp_text', 'pegel', 'abfluss']]
    realtime_df = realtime_df.rename(columns={'timestamp_text': 'timestamp'})

    payload = realtime_df.to_json(orient="records")
    print(f'Pushing {realtime_df.timestamp.count()} rows to ODS realtime API...')
    # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
    # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
    r = common.requests_post(url=credentials.ods_live_push_api_url, data=payload, verify=False)

print('Job successful!')
