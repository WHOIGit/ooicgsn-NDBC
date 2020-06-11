#!/bin/bash
if [ ! -d "Data/" ]
then
	mkdir Data
fi
python3 ndbc_data_packaging.py
