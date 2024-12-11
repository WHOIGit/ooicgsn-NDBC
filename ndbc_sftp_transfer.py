#!/usr/bin/env python3
import pysftp
from datetime import datetime
import pytz
import yaml
import os
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    # =========================================================================
    # Data directory path
    dataPath = '/home/ooiuser/ndbc/data'
    
    # Load the data from the yaml file
    user_info = yaml.safe_load(open('/home/ooiuser/ndbc/ndbc_sftp_info.yaml'))
    USERNAME = user_info['username']
    PASSWORD = user_info['password']
    HOST = user_info['host']
    PRIVATE_KEY_FILE = user_info['private_key_file']

    timestamp = datetime.now(tz=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
    date = datetime.now(tz=pytz.UTC).strftime('%Y%m%d')
    log = []

    # =========================================================================
    # Create a ftp session
    try:
        # Create an SSH connection
        ssh = paramiko.SSHClient()

        # Add new host key to the local HostKeys ojbect (in case of missing)
        # AutoAddPOlicy for missing host key to be set before connection setup
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Load the private key
        private_key = paramiko.Ed25519Key.from_private_key_file(PRIVATE_KEY_FILE, PASSWORD)
        
        # Connect the session
        ssh.connect(HOST, username=USERNAME, pkey=private_key)
        log.append(','.join((timestamp, f'Connected to {HOST}')))

        # Transfer the data using a SFTP connection
        sftp = ssh.open_sftp()

        # Create a list of the files to transfer
        xml_files = ["/".join((dataPath, x)) for x in os.listdir(dataPath)]
        
        # Transfer the files
        for file in xml_files:
            try:
                filename = file.split("/")[-1]
                sftp.put(file, filename)
                # Record transfer in log
                log.append(','.join((timestamp, file, 'success')))
            except:
                # If the transfer fails for whatever reason
                log.append(','.join((timestamp, xml_file, 'failed')))

    # If an FTP session can't be established
    except:
        # If a connection can't be made, record failure
        log.append(','.join((timestamp, f'Failed to connect to {HOST}')))

    # =========================================================================
    # Now record the log info
    with open(f'{dataPath}/../ndbc-logs/log_{date}.txt', 'a') as file:
        for line in log:
            file.write(f'{line}\n')


