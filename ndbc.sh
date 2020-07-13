#!/bin/bash
mkdir /home/ooiuser/ndbc-testing/NDBC/Data/
source activate NDBC && python3 ndbc_process_data.py \
source activate NDBC && python3 ndbc_transfer_data.py
rm -r /home/ooiuser/ndbc-testing/NDBC/Data/
