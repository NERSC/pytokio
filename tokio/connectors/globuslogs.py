"""Provides an interface for Globus and GridFTP transfer logs

Globus logs are ASCII files that generally look like::

    DATE=20190809091437.927804 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091437.884224 USER=glock FILE=/home/g/glock/results0.tar.gz BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
    DATE=20190809091438.022479 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091437.963894 USER=glock FILE=/home/g/glock/results1.tar.gz BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
    DATE=20190809091438.370175 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091438.314961 USER=glock FILE=/home/g/glock/results2.tar.gz BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226

The keys and values are pretty well demarcated, with the only hiccup being
around file names that contain spaces.
"""

import re
import time
import datetime
from tokio.common import to_epoch
from tokio.connectors.common import SubprocessOutputList

PEELER_REX = re.compile("^([A-Z]+)=(.*?)\s+([A-Z]+=.*)$")

class GlobusLog(SubprocessOutputList):
    """Interface into a Globus transfer log

    Parses a Globus transfer log which looks like::

        DATE=20190809091437.927804 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091437.884224 USER=glock FILE=/home/g/glock/results0.tar.gz BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
        DATE=20190809091438.022479 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091437.963894 USER=glock FILE=/home/g/glock/results1.tar.gz BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
        DATE=20190809091438.370175 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091438.314961 USER=glock FILE=/home/g/glock/results2.tar.gz BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226

    and represents the data in a list-like form::

        [
            {
                "BLOCK": 262144,
                "BUFFER": 87040,
                "CODE": "226",
                "DATE": 1565445916.0,
                "DEST": [
                    "198.125.208.14"
                ],
                "FILE": "/home/g/glock/results_08_F...",
                "HOST": "dtn11.nersc.gov",
                "NBYTES": 6341890048,
                "NL.EVNT": "FTP_INFO",
                "PROG": "globus-gridftp-server",
                "START": 1565445895.0,
                "STREAMS": 1,
                "STRIPES": 1,
                "TYPE": "STOR",
                "USER": "glock",
                "VOLUME": "/"
            },
            ...
        ]

    where each list item is a dictionary encoding a single transfer log line.
    The keys are exactly as they appear in the log file itself, and it is the
    responsibility of downstream analysis code to attribute meaning to each
    key.
    """

    def __init__(self, *args, **kwargs):
        super(GlobusLog, self).__init__(*args, **kwargs)
        self.load()

    @classmethod
    def from_str(cls, input_str):
        """Instantiates from a string
        """
        return cls(from_string=input_str)

    @classmethod
    def from_file(cls, cache_file):
        """Instantiates from a cache file
        """
        return cls(cache_file=cache_file)

    def load_str(self, input_str):
        """Parses text from a Globus FTP log

        Iterates through a multi-line string and converts each line into a
        dictionary of key-value pairs.

        Args:
            input_str (str): Multi-line string containing a single Globus log
                transfer record on each line.
        """
        for line in input_str.splitlines():
            rec = {}
            remainder = line

            while remainder:
                # we use a regex here because file paths may contain both spaces and =
                match = PEELER_REX.match(remainder)
                if not match:
                    key, value = remainder.split('=', 1)
                    remainder = ""
                else:
                    key = match.group(1)
                    value = match.group(2)
                    remainder = match.group(3)
                rec[key] = value

            # recast keys
            for key, transform in RECAST_KEYS.items():
                if key in rec:
                    rec[key] = transform(rec[key])

            if rec:
                self.append(rec)

def _listify_ips(ip_str):
    """Breaks a string encoding a list of destinations into a list of
    destinations

    Args: 
        ip_str (str): A list of destination hosts encoded as a string

    Returns:
        list: A list of destination host strings
    """
    if ip_str.startswith('['):
        return [x.strip() for x in ip_str.lstrip('[').rstrip(']').split(',')]
    return [ip_str]

RECAST_KEYS = {
    "DATE": lambda x: to_epoch(datetime.datetime.strptime(x, "%Y%m%d%H%M%S.%f"), float),
    "START": lambda x: to_epoch(datetime.datetime.strptime(x, "%Y%m%d%H%M%S.%f"), float),
    "BUFFER": int,
    "BLOCK": int,
    "NBYTES": int,
    "STREAMS": int,
    "STRIPES": int,
    "DEST": _listify_ips,
}
