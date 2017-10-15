#!/usr/bin/env python
"""
REST interface into the resources indexed by pytokio.
"""
# This REST API is generally modeled after the GitHub API and its conventions.
# See https://developer.github.com/v3/ for design guidelines.

import sys
import time
import json
import datetime
import argparse
import logging
import flask
import tokio
import tokio.connectors.hdf5
import tokio.tools.hdf5
import tokio.config

APP = flask.Flask(__name__)

DEFAULT_HDF5_DURATION_SECS = 60 * 15 # by default, retrieve data from last 15 minutes
MAX_HDF5_DURATION = datetime.timedelta(days=1)

def format_output(result):
    """
    Create a Flask response object from a return object
    """
    APP.logger.debug('accepts json? ' + str(flask.request.accept_mimetypes.accept_json))
    APP.logger.debug('accepts html? ' + str(flask.request.accept_mimetypes.accept_html))
    APP.logger.debug('accepts xhtml? ' + str(flask.request.accept_mimetypes.accept_xhtml))
    APP.logger.debug('accepts: ' + ' '.join([x for x in flask.request.accept_mimetypes.itervalues()]))
    if flask.request.accept_mimetypes.accept_json:
        APP.logger.debug("Returning json")
        return flask.jsonify(result)
    else: # flask.request.accept_mimetypes.accept_html:
        APP.logger.debug("Returning html")
        return flask.render_template(
            'json.html',
            return_data=result)

@APP.before_first_request
def init_logging(level=logging.DEBUG):
    """
    Initialize logging
    """
    log_format = '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
    logging.basicConfig(stream=sys.stdout,
                        level=level,
                        format=log_format)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(log_format))
    handler.setLevel(level)

    del APP.logger.handlers[:]
    APP.logger.addHandler(handler)
    APP.logger.setLevel(level)
    APP.logger.info("Log handler enabled")

def rest_error(err_code, err_message):
    """
    Populate the response object and return (error json, error object) tuple
    """
    result = {"message": err_message}
    response = format_output(result)
    response.status_code = err_code
    return response, result

def validate_file_system(file_system):
    """
    Verify that file system resource is valid and return (json, object) tuple
    """
    ### Return list of valid file systems
    if file_system is None:
        try:
            response = tokio.config.FSNAME_TO_H5LMT_FILE.keys()
        except AttributeError:
            raise
        return format_output(response), response

    ### Verify that specified file system is valid
    file_name = tokio.config.FSNAME_TO_H5LMT_FILE.get(file_system, None)
    if file_name is None:
        return rest_error(400, "Unknown file system")

    response = {"file_name": file_name}
    return format_output(response), response

def validate_hdf5_resource(resource_name):
    """
    Verify that HDF5 resource is valid and return (json, object) tuple
    """
    ### Return list of valid file systems
    if resource_name is None:
        response = tokio.connectors.hdf5.CONVERT_TO_V1_GROUPNAME.keys()
        return format_output(response), response

    ### Verify that specified file system is valid
    hdf5_resource = tokio.connectors.hdf5.CONVERT_TO_V1_GROUPNAME.get(resource_name, None)
    if hdf5_resource is None:
        return rest_error(400, "Unknown HDF5 resource")

    response = {"hdf5_resource": hdf5_resource}
    return format_output(response), response

def tokio_tool_hdf5(file_name, hdf5_resource, start, end):
    """
    Wrap the tokio.tools.hdf5 tool
    """
    try:
        result_df = tokio.tools.hdf5.get_dataframe_from_time_range(
            file_name, hdf5_resource, start, end)
    except OSError as error:
        ### Translate common input errors into REST errors
        if str(error).startswith("No relevant"):
            APP.logger.info(str(error))
            return rest_error(400, "No data found in time range")
        else:
            ### Don't expose unhandled exceptions to client
            raise

    try:
        ### TODO: sanitize flask.request.args values
        orient = flask.request.args.get('orient')
        if orient not in ('columns', 'index'):
            orient = 'columns'
        ### this is ugly, but the alternative is to to_dict then encode it with
        ### a custom json.JSONEncoder
        return format_output(json.loads(result_df.to_json(orient=orient)))
    except ValueError as error:
        return rest_error(400, str(error))

@APP.route('/hdf5/<file_system>/<resource>')
def hdf5_resource_route(file_system, resource):
    """
    GET data from a specific resource on a given file system

    Options:
       start: return results after this time (UTC seconds since epoch)
       end: return results before this time (UTC seconds since epoch)
       orient: orientation of resulting data (columns or index)
    """
    response_json, response = validate_file_system(file_system)
    file_name = response.get('file_name')
    if file_name is None:
        return format_output(response)

    response_json, response = validate_hdf5_resource(resource)
    hdf5_resource = response.get('hdf5_resource')
    if hdf5_resource is None:
        return format_output(response)

    ### Get start and end time and sanitize input

    try:
        end_time = long(flask.request.args.get('end', time.time()))
        start_time = long(flask.request.args.get('start', end_time - DEFAULT_HDF5_DURATION_SECS))
    except ValueError:
        return rest_error(400, "Non-numeric start/end time")

    datetime_start = datetime.datetime.fromtimestamp(start_time)
    datetime_end = datetime.datetime.fromtimestamp(end_time)
    if datetime_start >= datetime_end:
        return rest_error(400, "Invalid start/end time range")

    if (datetime_end - datetime_start) > MAX_HDF5_DURATION:
        return rest_error(400, "Start/end time cannot exceed %s"
                          % str(MAX_HDF5_DURATION))

    APP.logger.debug("Querying %s to %s" % (datetime_start, datetime_end))

    return tokio_tool_hdf5(file_name, hdf5_resource, datetime_start, datetime_end)

@APP.route('/hdf5/<file_system>/')
def file_system_route(file_system):
    """
    GET data from given file system
    """
    ### Validate file_system
    response_json, response = validate_file_system(file_system)
    file_name = response.get('file_name')
    if file_name is None:
        return format_output(response)

    ### Return list of valid resources
    response_json, _ = validate_hdf5_resource(None)
    return format_output(response)

@APP.route('/hdf5')
def hdf5_index():
    """
    GET file system time series data resources
    """
    ### Return list of valid file systems
    response_json, response = validate_file_system(None)
    return format_output(response)

@APP.route('/')
def index_to_list():
    """
    GET to generate list of endpoints
    """
    routes = []
    for rule in flask.current_app.url_map.iter_rules():
        route = rule.rule.lstrip('/')
        if route == '' or route.startswith('static'):
            continue
        
        ### Only display the top-level routes
        if len(route.split('/')) != 1:
            continue

        routes.append(route)
    return format_output(routes)

def index_to_dict():
    """
    GET to generate dictionary of endpoints and descriptions
    """
    routes = {} 
    for rule in flask.current_app.url_map.iter_rules():
        route = rule.rule
        if route == '/' or route.startswith('/static'):
            continue
        
        ### Only display the top-level routes
        if len(route.split('/')) != 2:
            continue

        ### Print first non-empty line of docstring for function
        for line in flask.current_app.view_functions.get(rule.endpoint).func_doc.splitlines():
            if line.strip() != "":
                description = line.strip()
                break
        routes[route] = description
    return format_output(routes)

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
    APP.logger.setLevel(logging.INFO)
    APP.run(host=args.host, port=args.port)

if __name__ == '__main__':
    launch_rest_api()
