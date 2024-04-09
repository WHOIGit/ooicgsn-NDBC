#!/bin/bash
##!/bin/bash -xe

PATHBASE='/home/ooiuser/ndbc'

dataPath="$PATHBASE/data"
codePath="$PATHBASE/ndbc-code"

start_time=$(date -u "+%Y-%m-%dT%H:%M:%S%Z")

echo "======== STARTING: $start_time ========"

# Rsync any new telemetered data to be processed 
rsync -aP ooiuser@ooi-cgpss1.whoi.net:/home/gi01sumo/D0010/cg_data/dcl11/metbk1/* /mnt/cg-data/raw/GI01SUMO/D00010/cg_data/dcl11/metbk1/
rsync -aP ooiuser@ooi-cgpss1.whoi.net:/home/gi01sumo/D0010/cg_data/dcl12/wavss/* /mnt/cg-data/raw/GI01SUMO/D00010/cg_data/dcl12/wavss/
rsync -aP ooiuser@ooi-cgpss1.whoi.net:/home/gi01sumo/D0010/cg_data/dcl12/metbk2/* /mnt/cg-data/raw/GI01SUMO/D00010/cg_data/dcl12/metbk2/

# Remove the data dir if not properly cleaned up on the last run
[ -d "$dataPath" ] && rm -r $dataPath && echo "Deleted $dataPath that was not removed after last run of ndbc.sh.";

mkdir $dataPath && echo "Creating new temporary data folder.";

source /home/ooiuser/anaconda3/bin/activate NDBC
echo "NDBC conda env activated."

echo "Starting processing..."
python3 $codePath/process_data.py && echo "Processing complete."

[ "$?" != "0" ] &&  logger -t "ndbc" "Error: /home/ooiuser/ndbc/ndbc-code/ndbc.sh - Processing data failed." && exit

echo "Starting transfer..."
python3 $codePath/ndbc_transfer_data.py && echo "Transfer complete."
[ "$?" != "0" ] &&  logger -t "ndbc" "Error: /home/ooiuser/ndbc/ndbc-code/ndbc.sh - Transferring data failed."

source /home/ooiuser/anaconda3/bin/deactivate && echo "NDBC conda env deactivated.";

rm -r $dataPath && echo "Removed temporary data folder.";

end_time=$(date -u "+%Y-%m-%dT%H:%M:%S%Z")
echo "======== DONE: $end_time ========"
