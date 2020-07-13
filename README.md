# NDBC

This repo hosts the code and supporting files for processing and pushing the OOI
Coastal & Global Scale Nodes (CGSN) METBK and WAVSS data from the surface buoys
at the Pioneer Array and Irminger Array to the NDBC data repository.

### Setup
#### 1. Create a directory structure that looks like this under `/home/ooiuser`:

```
ndbc
├── ndbc-code
├── ndbc-logs
└── ndbc_user_info.yaml
```

This is done via the following commands:

```
mkdir -p /home/ooiuser/ndbc/ndbc-logs
git clone https://github.com/reedan88/NDBC.git /home/ooiuser/ndbc/ndbc-code
```

Note, this "NDBC" repo is renamed to "ndbc-code" above.

#### 2. Copy the `ndbc_user_info.yaml` file into the `ndbc` directory

#### 3. Set up the appropriate python environment (NDBC) from the `ndbc_env.yaml` file.

Make sure that miniconda or anaconda3 has been installed.

```
conda env create -f ndbc_env.yaml
```

### Files in this repo
* ndbc_env.yaml - YAML file that sets up the appropriate conda environment
* ndbc_process_data.py - Python file which downloads the METBK and WAVSS data from the OOI OMS++ ERDDAP server for the past 3 hours, processes the data into the xml format needed by NDBC, and saves the xml files to a temporary data directory.
* ndbc_transfer_data.py - Python file which connects to the NDBC FTP server, opens the xml files in ASCII mode, and transfers the data to the NDBC FTP.
* ndbc.sh - Bash shell script which creates a temporary data directory, activates the NDBC python environment, executes the ndbc_process_data.py and ndbc_transfer_data.py scripts, then cleans up and removes the Data directory.


### Files outside this repo
* ndbc_user_info.yaml - YAML file which has the NDBC FTP server url, username, and password (this is not available on GitHub, and should be place in the same parent directory as this repo)

