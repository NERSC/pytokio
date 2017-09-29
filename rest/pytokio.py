#!/usr/bin/env python
"""
Wrappers around the pytokio API that provide a REST interface into some
connectors.
"""

import json
import datetime
import argparse
import bottle
import tokio
import tokio.connectors.hdf5
import tokio.tools.hdf5
import tokio.config

def rest_error(err_code, err_message, err_type=None, **kwargs):
    """
    Populate the response object and return the error json
    """
    bottle.response.status = err_code
    result = {"error": {"message": err_message}}
    if err_type is not None:
        result['error']['type'] = err_type
    return json.dumps(result)

def tokio_tool_hdf5(method, file_system, group, start, end):
    """
    Wrap the tokio.tools.hdf5 tool
    """
    file_name = tokio.config.FSNAME_TO_H5LMT_FILE.get(file_system, None)
    if file_name is None:
        return rest_error(400, "unknown file system '%s'" % file_system)

    if method == 'group':
        group_name = tokio.connectors.hdf5.CONVERT_TO_V1_GROUPNAME.get(group)
        if group_name is None:
            return rest_error(400, "unknown group name '%s'" % group)

        datetime_start = datetime.datetime.fromtimestamp(long(start))
        datetime_end = datetime.datetime.fromtimestamp(long(end))
        if datetime_start >= datetime_end:
            return rest_error(400, "invalid date range")

        result_df = tokio.tools.hdf5.get_dataframe_from_time_range(file_name,
                                                                   group_name,
                                                                   datetime_start,
                                                                   datetime_end)

        # TODO: check how much data is returned before it is returned
        try:
            ### TODO: sanitize bottle.request.query values
            return result_df.to_json(orient=bottle.request.query.get('orient', 'columns'),
                                     date_unit=bottle.request.query.get('date_unit', 's'))
        except ValueError as error:
            return rest_error(400, str(error))
    else:
        return rest_error(400, "unknown tools/hdf5 method '%s'" % method)

@bottle.get('/v1/<tool>/<file_system>/<method>/<group>/<start>/<end>')
def base_route(file_system, tool, method, group, start, end):
    """
    Provide API into tokio.tools
    """
    if tool == 'hdf5':
        return tokio_tool_hdf5(method, file_system, group, start, end)

    return rest_error(400, "unknown tool '%s'" % tool)

bottle.debug(True)

# Do NOT use bottle.run() with mod_wsgi
def launch_rest_api():
    """
    CLI interface to running the REST API service
    """
    parser = argparse.ArgumentParser(description="launch the pytokio REST interface",
                                     add_help=False)
    parser.add_argument("-h", "--host", type=str, default="localhost",
                        help="host/ip to bind")
    parser.add_argument("-p", "--port", type=int, default="18880",
                        help="port to bind")
    parser.add_argument("-w", "--watch", action='store_true',
                        help="watch for changes to this tool and restart when updated")
    args = parser.parse_args()
    bottle.run(host=args.host, port=args.port, reloader=args.watch)

if __name__ == '__main__':
    launch_rest_api()
else:
    application = bottle.default_app()
