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
    user_info = yaml.safe_load(open(f'{dataPath}/../ndbc_sftp_info.yaml'))
    USERNAME = user_info['username']
    PASSWORD = user_info['password']
    HOST = user_info['host']

    timestamp = datetime.now(tz=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
    date = datetime.now(tz=pytz.UTC).strftime('%Y%m%d')
    log = []

    # =========================================================================
    # Create a ftp session
    try:
        # Attempt to establish a connection
	with pysftp.Connection(HOST, username=USERNAME, private_key=PASSWORD) as sftp:
		log.append(','.join((timestamp, f'Connected to {HOST}')))

		# Create a list of the files to transfer
		xml_files = ["/".join((dataPath, x)) for x in os.listdir(dataPath)]

		# Try transfering the files
		for file in xml_files:
			try:
				sftp.put(file)
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


