#!/bin/bash
##!/bin/bash -xe

PATHBASE='/home/ooiuser/ndbc-testing/NDBC'
start_time=$(date -u "+%Y-%m-%dT%H:%M:%S%Z")

echo "======== STARTING: $start_time ========"

# Remove the Data dir if not properly cleaned up on the last run
[ -d "$PATHBASE/Data" ] && rm -r $PATHBASE/Data && echo "Deleted $PATHBASE/Data that was not removed after last run of ndbc.sh.";

mkdir $PATHBASE/Data && echo "Creating new temporary Data folder.";

source /home/ooiuser/anaconda3/bin/activate NDBC && echo "NDBC conda env activated." && echo "Starting processing..." && python3 $PATHBASE/ndbc_process_data.py && echo "Processing complete." && echo "Starting transfer..." && python3 $PATHBASE/ndbc_transfer_data.py && echo "Transfer complete.";

source /home/ooiuser/anaconda3/bin/deactivate && echo "NDBC conda env deactivated.";

rm -r $PATHBASE/Data && echo "Removed temporary Data folder.";

end_time=$(date -u "+%Y-%m-%dT%H:%M:%S%Z")
echo "======== DONE: $end_time ========"
