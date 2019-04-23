#!/bin/bash
#
#  Rename darshan logs to have a filename that is randomized but matches the
#  metadata encoding produced by Darshan.  Useful for generating output file
#  names for `darshan-convert --obfuscate` that still match standard darshan log
#  identification regular expressions.
#
jobid=1000000
for i in $@
do
    username=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w $((4 * RANDOM / 32767 + 8)) | head -n 1)
    exename="$(cat /dev/urandom | tr -dc 'a-z' | fold -w $((6 * RANDOM / 32767 + 4)) | head -n 1).x"
    let "jobid += RANDOM"
    newlogname="${username}_${exename}_id${jobid}_1-2-$((RANDOM * 86400 / 32768))-${RANDOM}${RANDOM}${RANDOM}_$((RANDOM * 5 / 32767)).darshan"
    mv -v $i $newlogname
done
