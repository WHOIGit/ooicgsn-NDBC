#!/usr/bin/env python3
import ftplib
from datetime import datetime
import pytz
import yaml
import os
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    # =========================================================================
    # Load the data from the yaml file
    user_info = yaml.load(open("ndbc_user_info.yaml"))
    USERNAME = user_info['username']
    PASSWORD = user_info['password']
    FTP = user_info['ftp']

    timestamp = datetime.now(tz=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
    date = datetime.now(tz=pytz.UTC).strftime('%Y%m%d')
    log = []

    # =========================================================================
    # Create a ftp session
    try:
        # Attempt to establish a connection
        session = ftplib.FTP(FTP, USERNAME, PASSWORD)
        log.append(','.join((timestamp, f'Connected to {FTP}')))

        for xml_file in [x for x in os.listdir('Data')]:
            # Attempt to transfer the xml files
            try:
                # Open file to transfer
                file = open(f'Data/{xml_file}', 'rb')

                # Transfer the file
                session.storlines(f'STOR {xml_file}', file)

                # Close the file
                file.close()

                # Record transfer in log
                log.append(','.join((timestamp, xml_file, 'success')))

            # If an individual file fails to transfer, record the error
            except:
                # If the transfer fails for whatever reason
                log.append(','.join((timestamp, xml_file, 'failed')))

        # Close the ftp session
        session.close()

    # If an FTP session can't be established
    except:
        # If a connection can't be made, record failure
        log.append(','.join((timestamp, f'Failed to connect to {FTP}')))

    # =========================================================================
    # Now record the log info
    with open(f'log_{date}.txt', 'a') as file:
        for line in log:
            file.write(f'{line}\n')


