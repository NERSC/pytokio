#!/usr/bin/env python
"""
Wrappers around the pytokio API that provide a REST interface into some
connectors.
"""

import json
import datetime
import bottle
import tokio
import tokio.connectors.hdf5
import tokio.tools.hdf5
import tokio.config

def tokio_tool_hdf5(method, fs, group, start, end):
    """
    Wrap the tokio.tools.hdf5 tool
    """
    if method == 'group':
        # TODO: check how much data is returned before it is returned
        file_name = tokio.config.FSNAME_TO_H5LMT_FILE.get(fs, None)
        if file_name is None:
            return json.dumps({'result': 'failure', 'reason': 'unknown file name'})
        # TODO: come up with a better way to do this (define group names in tokio.connectors.hdf5?)
        group_name = tokio.connectors.hdf5.CONVERT_TO_V1_GROUPNAME.get(group)
        if group_name is None:
            return json.dumps({'result': 'failure', 'reason': 'unknown group name'})
        datetime_start = datetime.datetime.fromtimestamp(long(start))
        datetime_end = datetime.datetime.fromtimestamp(long(end))
        if datetime_start >= datetime_end:
            # error: invalid relationship between start/end time
            return json.dumps({'result': 'failure', 'reason': 'invalid date range'})
        result = tokio.tools.hdf5.get_dataframe_from_time_range(file_name,
                                                                group_name,
                                                                datetime_start,
                                                                datetime_end)
        return result.to_json()
    else:
        return json.dumps({'result': 'failure', 'reason': 'unknown method'})

@bottle.get('/v1/<fs>/<tool>/<method>/<group>/<start>/<end>')
def base_route(fs, tool, method, group, start, end):
    """
    Provide API into tokio.tools
    """
    if tool == 'hdf5':
        result = tokio_tool_hdf5(method, fs, group, start, end)
        if result is None:
            bottle.response.status = 400
            return
        else:
            return result
    else:
        bottle.response.status = 400
        return

bottle.debug(True)

# Do NOT use bottle.run() with mod_wsgi
if __name__ == '__main__':
    bottle.run(host='localhost', port=18880, reloader=True)
else:
    application = bottle.default_app()
