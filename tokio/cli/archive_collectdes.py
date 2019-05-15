"""
Dump a lot of data out of ElasticSearch using the Python API and native
scrolling support.  Output either as native json from ElasticSearch or as
serialized TOKIO TimeSeries (TTS) HDF5 files.
"""

import os
import sys
import gzip
import json
import time
import datetime
import argparse
import warnings
import mimetypes
import collections
import multiprocessing

import dateutil.parser # because of how ElasticSearch returns time data
import dateutil.tz
import numpy

import tokio.debug
import tokio.timeseries
import tokio.connectors.collectd_es
import tokio.connectors.hdf5

SCHEMA_VERSION = "1"

DATASETS = collections.OrderedDict([
    ('datatargets/readrates', 'bytes/sec'),
    ('datatargets/writerates', 'bytes/sec'),
    ('datatargets/readoprates', 'ops/sec'),
    ('datatargets/writeoprates', 'ops/sec'),
    ('dataservers/memcached', 'bytes'),
    ('dataservers/membuffered', 'bytes'),
    ('dataservers/memfree', 'bytes'),
    ('dataservers/memused', 'bytes'),
    ('dataservers/memslab', 'bytes'),
    ('dataservers/memslab_unrecl', 'bytes'),
    ('dataservers/cpuload', '%'),
    ('dataservers/cpuuser', '%'),
    ('dataservers/cpusys', '%'),
    # these metadatasets _must_ be initialized _after_ their parent dataset
    ('dataservers/_num_cpuload', 'cpu count'),
    ('dataservers/_num_cpuuser', 'cpu count'),
    ('dataservers/_num_cpusys', 'cpu count'),
])

DATE_FMT = "%Y-%m-%dT%H:%M:%S"

# This is necessary because multiprocessing needs to be able to serialize the
# reducer that is passed back, but lambda functions are not pickleable.  So we
# pass back a string that maps to a lambda.
LAMBDA_MAPS = {
    'sum': lambda x, y: x + y,
}

def metadataset2dataset_key(metadataset_name):
    """Return the dataset name corresponding to a metadataset name

    Metadatasets are not ever stored in the HDF5 and instead are only used to
    store data needed to correctly calculate dataset values.  This function
    maps a metadataset name to its corresponding dataset name.

    Args:
        metadataset_name (str): Name of a metadataset
    Returns:
        str: Name of corresponding dataset name, or None if `metadataset_name`
        does not appear to be a metadataset name.
    """
    if '/_num_' not in metadataset_name:
        return None
    else:
        return metadataset_name.replace('/_num_', '/', 1)

def dataset2metadataset_key(dataset_key):
    """Return the metadataset name corresponding to a dataset name

    Args:
        dataset_name (str): Name of a dataset

    Returns:
        str: Name of corresponding metadataset name
    """
    return dataset_key.replace('/', '/_num_', 1)

def process_page(page):
    """
    Go through a list of docs and insert their data into a numpy matrix.  In
    the future this should be a flush function attached to the CollectdEs
    connector class.

    Args:
        page (dict): A single page of output from an Elasticsearch scroll
            query.  Should contain a ``hits`` key.
    """

    _time0 = time.time()
    inserts = []
    for doc in page:
        # basic validity checking
        if '_source' not in doc:
            warnings.warn("No _source in doc")
            print(json.dumps(doc, indent=4))
            continue
        source = doc['_source']

        # check to see if this is from plugin:disk
        if source['plugin'] == 'disk':
            # We jump through these hoops because most of TOKIO uses tz-unaware
            # local datetimes and we don't want to inject a bunch of dateutil
            # dependencies elsewhere
            # timezone-aware string
            # `-> tz-aware utc datetime
            #     `-> local datetime
            #         `-> tz-unaware local datetime
            timestamp = dateutil.parser.parse(source['@timestamp'])\
                                              .astimezone(dateutil.tz.tzlocal())\
                                              .replace(tzinfo=None)
            col_name = "%s:%s" % (source['hostname'], source['plugin_instance'])

            if source['collectd_type'] == 'disk_octets':
                val1 = source.get('read')
                val2 = source.get('write')
                if val1 is not None and val2 is not None:
                    inserts.append(('datatargets/readrates', timestamp, col_name, val1))
                    inserts.append(('datatargets/writerates', timestamp, col_name, val2))
            elif source['collectd_type'] == 'disk_ops':
                val1 = source.get('read')
                val2 = source.get('write')
                if val1 is not None and val2 is not None:
                    inserts.append(('datatargets/readoprates', timestamp, col_name, val1))
                    inserts.append(('datatargets/writeoprates', timestamp, col_name, val2))
        elif source['plugin'] == 'cpu' and 'value' in source:
            timestamp = dateutil.parser.parse(source['@timestamp'])\
                                              .astimezone(dateutil.tz.tzlocal())\
                                              .replace(tzinfo=None)
            val1 = source.get('value')
            if source['type_instance'] == 'idle':
                # note that we store (100 - idle) as load
                inserts.append(('dataservers/cpuload', timestamp, source['hostname'],
                                100.0 - val1, 'sum'))
                inserts.append(('dataservers/_num_cpuload', timestamp, source['hostname'],
                                1, 'sum'))
            elif source['type_instance'] == 'user':
                inserts.append(('dataservers/cpuuser', timestamp, source['hostname'],
                                val1, 'sum'))
                inserts.append(('dataservers/_num_cpuuser', timestamp, source['hostname'],
                                1, 'sum'))
            elif source['type_instance'] == 'system':
                inserts.append(('dataservers/cpusys', timestamp, source['hostname'],
                                val1, 'sum'))
                inserts.append(('dataservers/_num_cpusys', timestamp, source['hostname'],
                                1, 'sum'))
        elif source['plugin'] == 'memory' and 'value' in source:
            timestamp = dateutil.parser.parse(source['@timestamp'])\
                                              .astimezone(dateutil.tz.tzlocal())\
                                              .replace(tzinfo=None)
            val1 = source.get('value')
            if source['type_instance'] == 'cached':
                inserts.append(('dataservers/memcached', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'buffered':
                inserts.append(('dataservers/membuffered', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'free':
                inserts.append(('dataservers/memfree', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'used':
                inserts.append(('dataservers/memused', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'slab_recl':
                inserts.append(('dataservers/memslab', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'slab_unrecl':
                inserts.append(('dataservers/memslab_unrecl', timestamp, source['hostname'], val1))

    _timef = time.time()
    if tokio.debug.DEBUG:
        print("Extracted %d inserts in %.4f seconds" % (len(inserts), _timef - _time0))
        per_dataset = {}
        for insert in inserts:
            if insert[0] not in per_dataset:
                per_dataset[insert[0]] = 0
            per_dataset[insert[0]] += 1
        for dataset_name in sorted(per_dataset.keys()):
            print("  %6d entries for %s" % (per_dataset[dataset_name], dataset_name))
    return inserts

def update_datasets(inserts, datasets):
    """Insert list of tuples into a dataset

    Insert a list of tuples into a :class:`tokio.timeseries.TimeSeries` object serially

    Args:
        inserts (list of tuples): List of tuples which should be serially
            inserted into a dataset.  The tuples can be of the form

                * dataset name (str)
                * timestamp (:class:`datetime.datetime`)
                * column name (str)
                * value

            or

                * dataset name (str)
                * timestamp (:class:`datetime.datetime`)
                * column name (str)
                * value
                * reducer name (str)

            where

                * `dataset name` is the key used to retrieve a target
                  :class:`tokio.timeseries.TimeSeries` object from the `datasets`
                  argument
                * `timestamp` and `column name` reference the element to be udpated
                * value is the new value to insert into the given (`timestamp`,
                  `column name`) location within `dataset`.
                * `reducer name` is None (to just replace whatever value
                  currently exists in the (`timestamp`, `column name`) location,
                  or 'sum' to add `value` to the existing value.

        datasets (dict): Dictionary mapping dataset names (str) to
            :class:`tokio.timeseries.TimeSeries` objects

    Returns:
        int: number of elements in `inserts` which were not inserted because
        their timestamp value was out of the range of the dataset to be updated.
    """
    data_volume = {}
    errors = {}
    for key in datasets:
        data_volume[key] = 0.0
        errors[key] = 0

    for insert in inserts:
        try:
            if len(insert) == 4:
                (dataset_name, timestamp, col_name, value) = insert
                reducer = None
            else:
                (dataset_name, timestamp, col_name, value, reducer_name) = insert
                reducer = LAMBDA_MAPS.get(reducer_name)
        except ValueError:
            print(insert)
            raise

        if datasets[dataset_name].insert_element(timestamp, col_name, value, reducer):
            data_volume[dataset_name] += value
            tidx, cidx = datasets[dataset_name].get_insert_pos(timestamp, col_name)
        else:
            errors[dataset_name] += 1

    # Update dataset metadata
    for key in datasets:
        unit = DATASETS.get(key, "unknown")
        datasets[key].dataset_metadata.update({'version': SCHEMA_VERSION, 'units': unit})
        datasets[key].group_metadata.update({'source': 'collectd_disk'})

    index_errors = sum(errors.values())
    if index_errors > 0:
        warnings.warn("Out-of-bounds indices (%d total) were detected" % index_errors)

    if tokio.debug.DEBUG:
        for key in data_volume:
            data_volume[key] *= datasets[key].timestep
        update_str = "Added "
        update_str = "Added %(datatargets/readrates)d bytes/" \
            + "%(datatargets/readoprates)d ops of read, " \
            + "%(datatargets/writerates)d bytes/" \
            + "%(datatargets/writeoprates)d ops of write, " \
            + "%(dataservers/cpuload).1f%% of cpu load-seconds, " \
            + "%(dataservers/memused)d bytes of memory-seconds"
        print(update_str % data_volume)
    return index_errors

def reset_timeseries(timeseries, start, end, value=-0.0):
    """Zero out a region of a tokio.timeseries.TimeSeries dataset

    Args:
        timeseries (tokio.timeseries.TimeSeries): data from a subset should be zeroed
        start (datetime.datetime): Time at which zeroing of all columns in
            `timeseries` should begin
        end (datetime.datetime): Time at which zeroing all columns in
            `timeseries` should end (exclusive)
        value: value which should be set in every element being reset

    Returns:
        Nothing
    """
    index0, _ = timeseries.get_insert_pos(start, None)
    indexf, _ = timeseries.get_insert_pos(end, None)
    timeseries.dataset[index0:indexf, :] = value

def normalize_cpu_datasets(inserts, datasets):
    """Normalize CPU load datasets

    Divide each element of CPU datasets by the number of CPUs counted at each
    point in time.  Necessary because these measurements are reported on a
    per-core basis, but not all cores may be reported for each timestamp.

    Args:
        inserts (list of tuples): list of inserts that were used to populate
            datasets
        datasets (dict of TimeSeries): all of the datasets being populated

    Returns:
        Nothing
    """
    dataset_names = set(['dataservers/cpuload', 'dataservers/cpuuser', 'dataservers/cpusys'])
    num_dataset_names = {}

    norm_elements = {}
    for dataset_name in dataset_names:
        norm_elements[dataset_name] = set([])
        num_dataset_names[dataset_name] = dataset2metadataset_key(dataset_name)

    # build a set of all elements that must be divided
    for insert in inserts:
        (dataset_name, timestamp, col_name) = insert[0:3]
        if dataset_name in dataset_names:
            # get the position of this element to be inserted
            t_index, c_index = datasets[dataset_name].get_insert_pos(timestamp, col_name)
            if t_index is not None and c_index is not None:
                norm_elements[dataset_name].add((t_index, c_index))

    # now divide each element to be divided
    for dataset_name in dataset_names:
        num_dataset_name = num_dataset_names[dataset_name]
        for t_index, c_index in norm_elements[dataset_name]:
            datasets[dataset_name].dataset[t_index, c_index] /= \
                datasets[num_dataset_name].dataset[t_index, c_index]
        # convert NaNs (0.0 / 0.0) back to -0.0
        datasets[dataset_name].dataset[numpy.isnan(datasets[dataset_name].dataset)] = -0.0

def pages_to_hdf5(pages, output_file, init_start, init_end, query_start, query_end,
                  timestep, num_servers, devices_per_server, threads=1):
    """Stores a page from Elasticsearch query in an HDF5 file
    Take pages from ElasticSearch query and store them in output_file

    Args:
        pages (list): A list of page objects (dictionaries)
        output_file (str): Path to an HDF5 file in which page data should be
            stored
        init_start (datetime.datetime): Lower bound of time (inclusive) to be
            stored in the ``output_file``.  Used when creating a non-existent
            HDF5 file.
        init_end (datetime.datetime): Upper bound of time (inclusive) to be
            stored in the ``output_file``.  Used when creating a non-existent
            HDF5 file.
        query_start (datetime.datetime): Retrieve data greater than or equal to
            this time from Elasticsearch
        query_end (datetime.datetime); Retrieve data less than this time from
            Elasticsearch
        timestep (int): Time, in seconds, between successive sample intervals
            to be used when initializing ``output_file``
        num_servers (int): Number of discrete servers in the cluster.  Used
            when initializing ``output_file``.
        devices_per_server (int): Number of SSDs per server.  Used when
            initializing ``output_file``.
        threads (int): Number of parallel threads to utilize when parsing the
            Elasticsearch output
    """
    datasets = {}

    file_exists = False
    if os.path.isfile(output_file):
        file_exists = True

    with tokio.connectors.hdf5.Hdf5(output_file) as hdf5_file:
        schema_version = hdf5_file.get_version()

        # New files have a blank slate and should use the latest; existing files may
        # have orphaned duplicate data if two schemata are encoded at once
        if file_exists and schema_version != SCHEMA_VERSION:
            warnings.warn("%s existing version %s will be upgraded in-place to %s"
                % (output_file, schema_version, SCHEMA_VERSION))

        # Update to the latest schema version no matter what
        schema_version = SCHEMA_VERSION
        hdf5_file.attrs['version'] = SCHEMA_VERSION
        schema = tokio.connectors.hdf5.SCHEMA.get(schema_version)
        if schema is None:
            raise KeyError("Schema version %d in %s is not known by connectors.hdf5" % (SCHEMA_VERSION, output_file))

        # Initialize datasets
        for dataset_name in DATASETS:
            hdf5_dataset_name = schema.get(dataset_name)
            if hdf5_dataset_name is None:
                if '/_' not in dataset_name:
                    warnings.warn("Dataset %s in %s is not in schema version %s (passing through)" % (dataset_name, output_file, schema_version))
                hdf5_dataset_name = dataset_name
            if dataset_name.lstrip('/').startswith('datatargets'):
                num_columns = num_servers * devices_per_server
            else:
                num_columns = num_servers

            # If this is a metadataset, initialize it with the shape and columns of
            # the dataset it describes so that we can use the same index values for
            # both.  This requires the parent dataset's TimeSeries to already be
            # defined and attached, which requires DATASETS to be an OrderedDict so
            # that we aren't initializing metadatasets before datasets.
            real_dataset_name = metadataset2dataset_key(dataset_name)
            if real_dataset_name:
                if real_dataset_name not in datasets:
                    raise KeyError("Cannot init metadataset %s; dataset %s does not exist" %
                                   (dataset_name, real_dataset_name))
                global_start = datetime.datetime.fromtimestamp(datasets[real_dataset_name].timestamps[0])
                global_end = datetime.datetime.fromtimestamp(datasets[real_dataset_name].timestamps[-1] + datasets[real_dataset_name].timestep)
                timeseries = tokio.timeseries.TimeSeries(dataset_name=hdf5_dataset_name,
                                                         start=global_start,
                                                         end=global_end,
                                                         timestep=timestep,
                                                         num_columns=num_columns,
                                                         column_names=datasets[real_dataset_name].columns)

                # we can't update an average, so zero out all prior values that
                # we will be overwriting
                reset_timeseries(datasets[real_dataset_name], query_start, query_end)
            else:
                timeseries = hdf5_file.to_timeseries(dataset_name=hdf5_dataset_name)
                if timeseries is None:
                    timeseries = tokio.timeseries.TimeSeries(dataset_name=hdf5_dataset_name,
                                                             start=init_start,
                                                             end=init_end,
                                                             timestep=timestep,
                                                             num_columns=num_columns)
            datasets[dataset_name] = timeseries

        # Process all pages retrieved (this is computationally expensive)
        _time0 = time.time()
        updates = []
        if threads > 1:
            pool = multiprocessing.Pool(threads)
            for update in pool.imap_unordered(process_page, pages):
                updates.append(update)
            # explicitly terminate to prevent HDF5 locking problems caused by
            # un-gc'ed file handles
            pool.terminate()
        else:
            for page in pages:
                updates.append(process_page(page))
        _timef = time.time()
        _extract_time = _timef - _time0
        tokio.debug.debug_print("Extracted %d elements from %d pages in %.4f seconds" \
                                % (sum([len(x) for x in updates]),
                                   len(pages),
                                   _extract_time))

        # Take the processed list of data to insert and actually insert them
        _time0 = time.time()
        for update in updates:
            update_datasets(update, datasets)
        _timef = time.time()
        _update_time = _timef - _time0
        if tokio.debug.DEBUG:
            print("Inserted %d elements from %d pages in %.4f seconds" \
                % (len(updates), len(pages), _update_time))
            print("Processed %d pages in %.4f seconds" \
                % (len(pages), _extract_time + _update_time))

        for update in updates:
            normalize_cpu_datasets(update, datasets)

        # Write datasets out to HDF5 file
        _time0 = time.time()
        for dataset_name, dataset in datasets.items():
            if '/_' not in dataset_name:
                hdf5_file.commit_timeseries(dataset)

    if tokio.debug.DEBUG:
        print("Committed data to disk in %.4f seconds" % (time.time() - _time0))

def main(argv=None):
    """Entry point for the CLI interface
    """
    warnings.simplefilter('always', UserWarning) # One warning per invalid file

    # Parse CLI options
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("query_start", type=str,
                        help="start time of query in %s format" % DATE_FMT)
    parser.add_argument("query_end", type=str,
                        help="end time of query in %s format" % DATE_FMT)
    parser.add_argument('--init-start', type=str, default=None,
                        help='min timestamp if creating new output file, in %s format' % DATE_FMT
                        + ' (default: same as start)')
    parser.add_argument('--init-end', type=str, default=None,
                        help='max timestamp if creating new output file, in %s format' % DATE_FMT
                        + ' (default: same as end)')
    parser.add_argument('--debug', action='store_true',
                        help="produce debug messages")
    parser.add_argument('--num-nodes', type=int, default=288,
                        help='number of expected burst buffer nodes (default: 288)')
    parser.add_argument('--ssds-per-node', type=int, default=4,
                        help='number of SSDs in each BB node (default: 4)')
    parser.add_argument('--timestep', type=int, default=10,
                        help='collection frequency, in seconds (default: 10)')
    parser.add_argument('--timeout', type=int, default=30,
                        help='ElasticSearch timeout time (default: 30)')
    parser.add_argument('--threads', type=int, default=1,
                        help='parallel threads for document extraction (default: 1)')
    parser.add_argument('--input', type=str, default=None,
                        help="use cached ElasticSearch json as input")
    parser.add_argument("-o", "--output", type=str, default='output.hdf5',
                        help="output file (default: output.hdf5)")
    parser.add_argument('-h', '--host', type=str, default="localhost",
                        help="hostname of ElasticSearch endpoint (default: localhost)")
    parser.add_argument('-p', '--port', type=int, default=9200,
                        help="port of ElasticSearch endpoint (default: 9200)")
    parser.add_argument('-i', '--index', type=str, default='cori-collectd-*',
                        help='ElasticSearch index to query (default:cori-collectd-*)')
    args = parser.parse_args(argv)

    if args.debug:
        tokio.debug.DEBUG = True

    # Convert CLI options into datetime
    try:
        query_start = datetime.datetime.strptime(args.query_start, DATE_FMT)
        query_end = datetime.datetime.strptime(args.query_end, DATE_FMT)
        init_start = query_start
        init_end = query_end
        if args.init_start:
            init_start = datetime.datetime.strptime(args.init_start, DATE_FMT)
        if args.init_end:
            init_end = datetime.datetime.strptime(args.init_end, DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % DATE_FMT)
        raise

    # Basic input bounds checking
    if query_start >= query_end:
        raise Exception('query_start >= query_end')
    elif init_start >= init_end:
        raise Exception('init_start >= init_end')
    elif args.timestep < 1:
        raise Exception('--timestep must be > 0')

    # Read input from a cached json file (generated previously via the --json
    # option) or by querying ElasticSearch?
    if args.input is None:
        ### Try to connect
        esdb = tokio.connectors.collectd_es.CollectdEs(
            host=args.host,
            port=args.port,
            index=args.index,
            timeout=args.timeout)

        pages = None
        for plugin_query in [tokio.connectors.collectd_es.QUERY_CPU_DATA,
                             tokio.connectors.collectd_es.QUERY_DISK_DATA,
                             tokio.connectors.collectd_es.QUERY_MEMORY_DATA]:
            esdb.query_timeseries(plugin_query,
                                  query_start,
                                  query_end)
            if pages is None:
                pages = esdb.scroll_pages
            else:
                pages += esdb.scroll_pages

            tokio.debug.debug_print("Loaded results from %s:%s" % (args.host, args.port))
            pages_to_hdf5(pages=pages,
                          output_file=args.output,
                          init_start=init_start,
                          init_end=init_end,
                          query_start=query_start,
                          query_end=query_end,
                          timestep=args.timestep,
                          num_servers=args.num_nodes,
                          devices_per_server=args.ssds_per_node,
                          threads=args.threads)
    else:
        _, encoding = mimetypes.guess_type(args.input)
        if encoding == 'gzip':
            input_file = gzip.open(args.input, 'r')
        else:
            input_file = open(args.input, 'r')
        pages = json.load(input_file)
        input_file.close()
        tokio.debug.debug_print("Loaded results from %s" % args.input)
        pages_to_hdf5(pages=pages,
                      output_file=args.output,
                      init_start=init_start,
                      init_end=init_end,
                      query_start=query_start,
                      query_end=query_end,
                      timestep=args.timestep,
                      num_servers=args.num_nodes,
                      devices_per_server=args.ssds_per_node,
                      threads=args.threads)

    print("Wrote output to %s" % args.output)
