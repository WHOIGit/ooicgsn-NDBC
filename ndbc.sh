#!/bin/bash
mkdir Data/
source activate NDBC && python3 ndbc_data_packaging.py \
source activate NDBC && python3 ndbc_transfer_data.py
rm -r Data/
