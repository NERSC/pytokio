#!/usr/bin/env python

import subprocess
import errno
import nose
import tokio.connectors.darshan

SKIP_DARSHAN = None

def needs_darshan(func):
    """
    Need to check if darshan-parser is available; if not, just skip all
    Darshan-related tests
    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN is not None:
        return func
    try:
        subprocess.check_output(tokio.connectors.darshan.DARSHAN_PARSER_BIN, stderr=subprocess.STDOUT)
    except OSError as error:
        if error[0] == errno.ENOENT:
            SKIP_DARSHAN = True
    except subprocess.CalledProcessError:
        # this is ok--there's no way to make darshan-parser return zero without
        # giving it a real darshan log
        pass
    return func

def check_darshan():
    global SKIP_DARSHAN
    if SKIP_DARSHAN:
        raise nose.SkipTest("%s not available" % tokio.connectors.darshan.DARSHAN_PARSER_BIN)
