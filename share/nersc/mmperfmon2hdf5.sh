#!/usr/bin/env bash
#
#  Tool to archive a day's worth of mmperfmon data into a TOKIO Time Series file
#

HERE=$(dirname $(readlink -f ${BASH_SOURCE[0]}))

PYTOKIO_HOME=${PYTOKIO_HOME:-$(readlink -f ${HERE}/../..)}
ARCHIVE_MMPERFMON=${ARCHIVE_MMPERFMON:-$PYTOKIO_HOME/bin/archive_mmperfmon.py}
TIMESTEP=${TIMESTEP:-60}

declare -A MMPERFMON_DIR_BASES
MMPERFMON_DIR_BASES[projecta]="/global/projecta/iotest/kkr/gpfs-for-glenn/output"
MMPERFMON_DIR_BASES[projectb]="/global/projectb/iotest/kkr/gpfs-for-glenn/output"
MMPERFMON_DIR_BASES[project2]="/global/project/iotest/kkr/gpfs-for-glenn/output"

# you shouldn't have to modify anything below here

# Parse arguments
usage() { echo "$0 usage:" && grep " .)\ #" $0; exit 0; }

FORCE=0

while getopts "fh" arg; do
  case $arg in
    f) # do not abort on invalid dates
      FORCE=1
      ;;
    h | *) # display help
      usage
      exit 0
      ;;
  esac
done
shift $((OPTIND-1))

FSNAME=$1
DATE=$2

if [ -z "$MMPERFMON_DIR_BASE" ]; then
    MMPERFMON_DIR_BASE="${MMPERFMON_DIR_BASES[$FSNAME]}"
fi

if [ -z "$DATE" -o -z "$MMPERFMON_DIR_BASE" ]; then
    echo "Syntax: $0 fsname YYYY-MM-DD [YYYY-MM-DD]" >&2
    echo "" >&2
    echo "where fsname is one of: ${!MMPERFMON_DIR_BASES[@]}" >&2
    exit 1
fi

END_DATE=$3
if [ -z "$END_DATE" ]; then
    END_DATE="$DATE"
fi
END_EPOCH=$(date -d "$END_DATE" "+%s")

today=$(date -d "$DATE" "+%Y-%m-%d")
today_epoch=$(date -d "$today" "+%s")

if [ $today_epoch -lt $END_EPOCH ]; then
    one_day=86400
else
    one_day=-86400
fi

while true; do
    if [ $one_day -gt 0 -a "$today_epoch" -gt "$END_EPOCH" ]; then
        break
    elif [ $one_day -lt 0 -a "$today_epoch" -lt "$END_EPOCH" ]; then
        break
    fi
    yesterday=$(date -d "$today - 1 day" "+%Y-%m-%d")
    tomorrow=$(date -d "$today + 1 day" "+%Y-%m-%d")
    echo "[$(date)] Archiving ${FSNAME} for ${today}"

    if [ ! -d "$today" ]; then
        mkdir -vp "$today"
    fi

    output_file="$today/${FSNAME}.hdf5"

    tstart=$(date +%s)
    ${ARCHIVE_MMPERFMON} --init-start "${today}T00:00:00" \
                         --init-end "${tomorrow}T00:00:00" \
                         --timestep ${TIMESTEP} \
                         --filesystem "$FSNAME" \
                         --output "$output_file" \
                         "${today}T00:00:00" \
                         "${tomorrow}T00:00:00"
#                        "${today}T23:59:59"

    ret=$?
    tend=$(date +%s)

    if [ ! -f "$output_file" ]; then
        echo "[$(date)] ERROR: did not create $output_file" >&2
        if [ ! $FORCE ]; then
            exit $ret
        fi
    else
        echo "[$(date)] Wrote output to $output_file in $((tend - tstart)) seconds"
    fi

    today_epoch=$((today_epoch + one_day))
    today=$(date -d "$today + $one_day seconds" "+%Y-%m-%d")
done
