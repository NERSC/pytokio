#!/bin/bash -l
#
#  Wrapper for aggr_h5lmt to extract the Lustre server-side activity logged
#  during a specific time/date range.  Useful to get high-level metrics about
#  file system activity when a specific job was running. 
#
#  Syntax:
#
#    ./this_script "2017-03-01 12:37:15" "2017-03-01 14:12:57" scratch3
#
#  The third argument is optional; if omitted, return summaries from all file
#  systems.
#

if [ -z "$3" ]; then
    fs_files="edison_snx11025.h5lmt edison_snx11035.h5lmt edison_snx11036.h5lmt cori_snx11168.h5lmt"
elif [ "$3" == "scratch1" ]; then
    fs_files="edison_snx11025.h5lmt"
elif [ "$3" == "scratch2" ]; then
    fs_files="edison_snx11035.h5lmt"
elif [ "$3" == "scratch3" ]; then
    fs_files="edison_snx11036.h5lmt"
elif [ "$3" == "cscratch" ]; then
    fs_files="cori_snx11168.h5lmt"
else
    echo "ERROR: unknown file system [$3]" >&2
    exit 1
fi

./aggr_h5lmt.py \
    --start "$1" \
    --end "$2" \
    --summary \
    --bins 1 \
    $fs_files
