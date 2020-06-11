# NDBC
This repo host the code and supporting files for processing and pushing the OOI
Coastal & Global Scale Nodes (CGSN) METBK and WAVSS data from the surface buoys
at the Pioneer Array and Irminger Array to the NDBC data repository.

### Setup
First, set up the appropriate python environment (NDBC) from the ndbc_env.yaml
file. Make sure that miniconda has been installed.

```
conda env create -f ndbc_env.yaml
```

### Files
* ndbc_env.yaml - YAML file that sets up the appropriate conda environment
* ndbc_user_info.yaml - YAML file which has the NDBC FTP server url, username, and password (this is not available on GitHub)
* ndbc_process_data.py - Python file which downloads the METBK and WAVSS data from the OOI OMS++ ERDDAP server for the past 3 hours, processes the data into the xml format needed by NDBC, and saves the xml files to a temporary data directory.
* ndbc_transfer_data.py - Python file which connects to the NDBC FTP server, opens the xml files in ASCII mode, and transfers the data to the NDBC FTP.
* ndbc.sh - Bash shell script which creates a temporary data directory, activates the NDBC python environment, executes the ndbc_process_data.py and ndbc_transfer_data.py scripts, then cleans up and removes the Data directory.
