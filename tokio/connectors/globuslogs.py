"""Connect to Globus and GridFTP transfer logs
"""

import time
import datetime
from tokio.connectors.common import SubprocessOutputList

class GlobusLog(SubprocessOutputList):
    """Interface into a Globus transfer log

    Parses a Globus transfer log which looks like::

        DATE=20190809091437.927804 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091437.884224 USER=jliang FILE=/home/g/genwang/24IDC/AMA/Done_3p5/002016_AMA.tar.idx BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
        DATE=20190809091438.022479 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091437.963894 USER=jliang FILE=/home/g/genwang/24IDC/AMA/Done_3p5/002024_AMA.tar.idx BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
        DATE=20190809091438.089833 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091438.044848 USER=jliang FILE=/home/g/genwang/24IDC/AMA/Done_3p5/002032_AMA.tar.idx BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
        DATE=20190809091438.151535 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091438.111117 USER=jliang FILE=/home/g/genwang/24IDC/AMA/Done_3p5/002040_AMA.tar.idx BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
        DATE=20190809091438.231634 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091438.170837 USER=jliang FILE=/home/g/genwang/24IDC/AMA/Done_3p5/002048_AMA.tar.idx BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
        DATE=20190809091438.294737 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091438.252179 USER=jliang FILE=/home/g/genwang/24IDC/AMA/Done_3p5/002056_AMA.tar.idx BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226
        DATE=20190809091438.370175 HOST=dtn11.nersc.gov PROG=globus-gridftp-server NL.EVNT=FTP_INFO START=20190809091438.314961 USER=jliang FILE=/home/g/genwang/24IDC/AMA/Done_3p5/002064_AMA.tar.idx BUFFER=235104 BLOCK=262144 NBYTES=35616 VOLUME=/ STREAMS=4 STRIPES=1 DEST=[0.0.0.0] TYPE=RETR CODE=226

    and represents the data in a dictionary-like form::

        ...

    """

    def __init__(self, *args, **kwargs):
        super(GlobusLog, self).__init__(*args, **kwargs)
        self.load()

    @classmethod
    def from_str(cls, input_str):
        """Instantiate from a string
        """
        return cls(from_string=input_str)

    @classmethod
    def from_file(cls, cache_file):
        """Instantiate from a cache file
        """
        return cls(cache_file=cache_file)

    def load_str(self, input_str):
        """Parse text from a Globus FTP log
        DATE=20190809091438.370175
        HOST=dtn11.nersc.gov
        PROG=globus-gridftp-server
        NL.EVNT=FTP_INFO
        START=20190809091438.314961
        USER=xyzabc
        FILE=/home/x/xyzabc/24IDC/AMA/Done_3p5/002064_AMA.tar.idx
        BUFFER=235104
        BLOCK=262144
        NBYTES=35616
        VOLUME=/
        STREAMS=4
        STRIPES=1
        DEST=[0.0.0.0]
        TYPE=RETR
        CODE=226
        """
        for line in input_str.splitlines():
            rec = {}
            remainder = line
            while remainder:
                key, remainder = remainder.split("=", 1)
                if key in ("FILE", "VOLUME"):
                    # allow for spaces in paths
                    if '=' not in remainder:
                        value = remainder
                        remainder = ""
                    else:
                        value, remainder = remainder.split('=', 1)
                        value, nextkey = value.rsplit(None, 1)
                        remainder = nextkey + "=" + remainder
                elif ' ' in remainder:
                    value, remainder = remainder.split(None, 1)
                else:
                    value = remainder
                    remainder = ""
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
    "DATE": lambda x: time.mktime(datetime.datetime.strptime(x, "%Y%m%d%H%M%S.%f").timetuple()),
    "START": lambda x: time.mktime(datetime.datetime.strptime(x, "%Y%m%d%H%M%S.%f").timetuple()),
    "BUFFER": int,
    "BLOCK": int,
    "NBYTES": int,
    "STREAMS": int,
    "STRIPES": int,
    "DEST": _listify_ips,
}
