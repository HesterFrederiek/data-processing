import glob
import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp
import pandas as pd
from staka_abstimmungen import credentials
from staka_abstimmungen.src.etl_details import calculate_details
from staka_abstimmungen.src.etl_kennzahlen import calculate_kennzahlen
import smtplib


def main():
    logging.info(f'Reading control.csv...')
    df = pd.read_csv(os.path.join(credentials.path, 'control.csv'), sep=';', parse_dates=['Ignore_changes_before', 'Embargo', 'Ignore_changes_after'])
    active_abst = df.query('Active == True').copy(deep=True)
    active_active_size = active_abst.Active.size
    what_changed = {'updated_ods_datasets': [], 'datasets_changed_to_public': [], 'send_update_email': False}
    if active_active_size == 1:
        abst_date = active_abst.Abstimmungs_datum[0]
        logging.info(f'Processing Abstimmung for date {abst_date}...')
        do_process, make_live_public = check_embargos(active_abst, active_active_size)
        logging.info(f'Should we check for changes in data files now? {do_process}')
        if do_process:
            active_files = find_data_files_for_active_abst(active_abst)
            data_files_changed = have_data_files_changed(active_files)            
            if data_files_changed:
                details_changed, kennz_changed = calculate_and_upload(active_files)
                # todo: Create live datasets in ODS as a copy of the test datasets if they do not exist yet.
                # todo: Use ods realtime push instead of FTP pull.
                what_changed = publish_datasets(active_abst, details_changed, kennz_changed, what_changed=what_changed)
                for file in active_files:
                    ct.update_hash_file(os.path.join(credentials.path, file))
                
            logging.info(f'Is it time to make live datasets public? {make_live_public}. ')
            if make_live_public:
                what_changed = make_datasets_public(active_abst, active_files, what_changed)
            send_update_email(what_changed)
    elif active_active_size == 0:
        logging.info(f'No active Abstimmung, nothing to do for the moment. ')
    elif active_active_size > 1:
        raise NotImplementedError('Only one Abstimmung must be active at any time!')
    logging.info(f'Job Successful!')


def send_update_email(what_changed):
    text = ''
    if len(what_changed['updated_ods_datasets']) > 0:
        what_changed['send_update_email'] = True
        text += f'Updated ODS Datasets: \n'
        for ods_id in what_changed['updated_ods_datasets']:
            text += f'- {ods_id}: https://data.bs.ch/explore/dataset/{ods_id} \n'
    if len(what_changed['datasets_changed_to_public']) > 0:
        what_changed['send_update_email'] = True
        text += f'Datasets changed from restricted to domain (public): \n'
        for ods_id in what_changed['datasets_changed_to_public']:
            text += f'- {ods_id}: https://data.bs.ch/explore/dataset/{ods_id} \n'
    logging.info(f'Is it time to send an update email? {what_changed["send_update_email"]}')
    if what_changed['send_update_email']:
        text += f'\n\nKind regards, \nYour automated Open Data Basel-Stadt Python Job'
        msg = common.email_message(subject='Abstimmungen: Updates have been automatically pushed to ODS', text=text)
        host = credentials.email_server
        smtp = smtplib.SMTP(host)
        smtp.sendmail(from_addr='opendata@bs.ch', to_addrs=credentials.email_receivers, msg=msg.as_string())
        smtp.quit()
        logging.info(f'Update email sent: ')
        logging.info(text)
    return what_changed['send_update_email'], text


def make_datasets_public(active_abst, active_files, what_changed):
    vorlage_in_filename = [f for f in active_files if 'Vorlage' in f]
    logging.info(f'Number of data files with "Vorlage" in the filename: {len(vorlage_in_filename)}. If 0: setting live ods datasets to public...')
    if len(vorlage_in_filename) == 0:
        for ods_id in [active_abst.ODS_id_Kennzahlen_Live[0], active_abst.ODS_id_Details_Live[0]]:
            policy_changed, r = odsp.ods_set_general_access_policy(ods_id, 'domain')
            if policy_changed:
                what_changed['datasets_changed_to_public'].append(ods_id)
    return what_changed


def publish_datasets(active_abst, details_changed, kennz_changed, what_changed):
    if kennz_changed:
        logging.info(f'Kennzahlen have changed, publishing datasets (Test and live)...')
        for ods_id in [active_abst.ODS_id_Kennzahlen_Live[0], active_abst.ODS_id_Kennzahlen_Test[0]]:             
            odsp.publish_ods_dataset_by_id(ods_id)
            what_changed['updated_ods_datasets'].append(ods_id)        
    if details_changed:
        logging.info(f'Details have changed, publishing datasets (Test and live)...')
        for ods_id in [active_abst.ODS_id_Details_Live[0], active_abst.ODS_id_Details_Test[0]]: 
            odsp.publish_ods_dataset_by_id(ods_id)
            what_changed['updated_ods_datasets'].append(ods_id)
    return what_changed


def calculate_and_upload(active_files):
    # todo: Use ODS Realtime API to push data. Using FTP pull often required de- and re-publishing because of perceived schema changes
    details_abst_date, de_details = calculate_details(active_files)
    details_export_file_name = os.path.join(credentials.path, 'data-processing-output', f'Abstimmungen_Details_{details_abst_date}.csv')
    details_changed = upload_ftp_if_changed(de_details, details_export_file_name)

    kennz_abst_date, df_kennz = calculate_kennzahlen(active_files)
    kennz_file_name = os.path.join(credentials.path, 'data-processing-output', f'Abstimmungen_{kennz_abst_date}.csv')
    kennz_changed = upload_ftp_if_changed(df_kennz, kennz_file_name)
    return details_changed, kennz_changed


def have_data_files_changed(active_files):
    data_files_changed = False
    for file in active_files:
        if ct.has_changed(os.path.join(credentials.path, file)):
            data_files_changed = True
    logging.info(f'Are there any changes in the active data files? {data_files_changed}.')
    return data_files_changed


def find_data_files_for_active_abst(active_abst):
    data_files = get_latest_data_files()
    abst_datum_string = active_abst.Abstimmungs_datum[0].replace('-', '')
    active_files = [f for f in data_files if abst_datum_string in f]
    logging.info(f'We have {len(active_files)} data files for the current Abstimmung: {active_files}. ')
    return active_files


def check_embargos(active_abst, active_active_size):
    logging.info(f'Found {active_active_size} active Abstimmung.')
    for column in ['Ignore_changes_before', 'Embargo', 'Ignore_changes_after']:
        active_abst[column] = active_abst[column].dt.tz_localize('Europe/Zurich')
    now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    do_process = active_abst.Ignore_changes_before[0] <= now_in_switzerland < active_abst.Ignore_changes_after[0]
    make_live_public = active_abst.Embargo[0] <= now_in_switzerland < active_abst.Ignore_changes_after[0]
    return do_process, make_live_public


def upload_ftp_if_changed(df, file_name):
    print(f'Exporting to {file_name}...')
    df.to_csv(file_name, index=False)
    has_changed = ct.has_changed(file_name)
    if has_changed:
        common.upload_ftp(file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'wahlen_abstimmungen/abstimmungen')
    return has_changed


def get_latest_data_files():
    data_file_names = []
    for pattern in ['*_EID_????????*.xlsx', '*_KAN_????????*.xlsx']:
        file_list = glob.glob(os.path.join(credentials.path, pattern))
        if len(file_list) > 0:
            latest_file = max(file_list, key=os.path.getmtime)
            data_file_names.append(os.path.basename(latest_file))
    return data_file_names


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
