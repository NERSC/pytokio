"""Retrieves mmperfmon counters and store them in TOKIO Timeseries format

Command-line tool that loads a :class:`tokio.connectors.mmperfmon.Mmperfmon`
object and encodes it as a TOKIO TimeSeries object.  Syntax to create a new
HDF5 is::

    $ archive_mmperfmon --timestep=60 --init-start 2019-05-15T00:00:00 \\
        --init-end 2019-05-16T00:00:00 mmperfmon.2019-05-15.tgz

where _mmperfmon.2019-05-15.tgz_ is one or more files that can be loaded by
:meth:`tokio.connectors.mmperfmon.Mmperfmon.from_file`.

When updating an existing HDF5 file, the minimum required syntax is::

    $ archive_mmperfmon --timestep=60 mmperfmon.2019-05-15.tgz

The init start/end times are only required when creating an empty HDF5 file.
"""

import re
import sys
import operator
import datetime
import argparse
import warnings
import tokio.debug
import tokio.config
import tokio.tools.nersc_mmperfmon
import tokio.timeseries
import tokio.connectors.mmperfmon
import tokio.connectors.hdf5

DATE_FMT = "%Y-%m-%dT%H:%M:%S"
DATE_FMT_PRINT = "YYYY-MM-DDTHH:MM:SS"

SCHEMA_VERSION = "1"

COUNTER_MAP = {
    "gpfs_nsdds_bytes_read": "readbytes",
    "gpfs_nsdds_bytes_written": "writebytes",
    "cpu_sys": "cpusys",
    "cpu_user": "cpuuser",
    "mem_free_bytes": "memfree",
    "mem_total_bytes": "memtotal",
    "net_r_bytes": "netinbytes", # not implemented
    "net_s_bytes": "netoutbytes", # not implemented
}

UNITS_MAP = {
    "cpusys": "%",
    "cpuuser": "%",
    "readbytes": "bytes",
    "writebytes": "bytes",
    "memfree": "bytes",
    "memtotal": "bytes",
    "netinbytes": "bytes", # not implemented
    "netoutbytes": "bytes", # not implemented
}

class Archiver(dict):
    """A dictionary containing TimeSeries objects

    Contains the TimeSeries objects being populated from a remote data source.
    Implemented as a class so that a single object can store all of the
    TimeSeries objects that are generated by multiple method calls.
    """
    def __init__(self, init_start, init_end, timestep, num_luns, num_servers, *args, **kwargs):
        """Initializes the archiver and stores its settings

        Args:
            init_start (datetime.datetime): Lower bound of time to be archived,
                inclusive
            init_end (datetime.datetime): Upper bound of time to be archived,
                exclusive
            timestep (int): Number of seconds between successive data points.
            num_luns (int or None): Number of LUNs expected to appear in mmperfmon
                outputs.  If None, autodetect.
            num_servers (int or None): Number of NSD servers expected to appear in
                mmperfmon outputs.  If None, autodetect.
        """
        super(Archiver, self).__init__(*args, **kwargs)
        self.init_start = init_start
        self.init_end = init_end
        self.timestep = timestep
        self.lun_map = None
        self.server_map = None
        self.num_luns = num_luns
        self.num_servers = num_servers
        self.lun_types = {}     # cached mapping between LUN names and their types
        self.server_types = {}  # cached mapping between server names and their types

        self.schema = tokio.connectors.hdf5.SCHEMA.get(SCHEMA_VERSION)
        if self.schema is None:
            raise KeyError("Schema version %d is not known by connectors.hdf5" % SCHEMA_VERSION)

    def init_dataset(self, dataset_name, columns):
        """Initialize an empty dataset within self

        Creates and attaches a TimeSeries object to self

        Args:
            dataset_name (str): name of dataset to be initialized
            columns (list of str): columns to initialize
        """
        hdf5_dataset_name = self.schema.get(dataset_name)
        if hdf5_dataset_name is None:
            warnings.warn("Skipping %s (not in schema)" % dataset_name)
        else:
            self[dataset_name] = tokio.timeseries.TimeSeries(
                dataset_name=hdf5_dataset_name,
                start=self.init_start,
                end=self.init_end,
                timestep=self.timestep,
                num_columns=len(columns),
                column_names=columns)
            self[dataset_name].sort_columns()

    def init_datasets(self, mmpm):
        """Initialize all datasets that can be created from an Mmperfmon instance

        This method examines an mmpm and identifies all TimeSeries datasets that
        can be derived from it, then calculates the dimensions of said datasets
        based on how many unique columns were found.  This is required because
        the precise number of columns is difficult to generalize a priori on SAN
        file systems with arbitrarily connected LUNs and servers.

        Also caches the mappings between LUN and NSD server names and their
        functions (data or metadata).

        Args:
            mmpm (tokio.connectors.mmperfmon.Mmperfmon): Object from which
                possible datasets should be identified and sized.
        """
        num_luns = set([])
        num_servers = set([])
        min_timestamp = None
        max_timestamp = None

        # first figure out the dimensions of each dataset and cache type maps
        datasets = {}
        for timestamp, fqhosts in mmpm.items():
            timestamp_int = int(timestamp)
            if min_timestamp is None or timestamp_int < min_timestamp:
                min_timestamp = timestamp_int
            if max_timestamp is None or timestamp_int > max_timestamp:
                max_timestamp = timestamp_int
            for fqhost, counters in fqhosts.items():
                num_servers.add(fqhost)
                server_type = self.server_type(fqhost)
                for counter, value in counters.items():
                    # identify non-scalar counters
                    if isinstance(value, dict):
                        for instance in value.keys():
                            column = fqhost + ":" + instance
                            lun_type = self.lun_type(instance)
                            if not lun_type:
                                continue
                            num_luns.add(instance)

                            dataset_name = "%ss/%s" % (lun_type, COUNTER_MAP.get(counter, counter))
                            if dataset_name not in datasets:
                                datasets[dataset_name] = set([])
                            datasets[dataset_name].add(column)
                    else:
                        canonical_counter = COUNTER_MAP.get(counter, counter)
                        dataset_name = "%ss/%s" % (server_type, canonical_counter)
                        if dataset_name not in datasets:
                            datasets[dataset_name] = set([])
                        datasets[dataset_name].add(fqhost)

                        # little hacky bits to patch together missing datasets
                        if canonical_counter == 'cpuuser':
                            dataset_name = "%ss/cpuload" % server_type
                            if dataset_name not in datasets:
                                datasets[dataset_name] = set([])
                            datasets[dataset_name].add(fqhost)

        # figure out number of luns and servers if necessary
        if self.num_luns is None:
            self.num_luns = len(num_luns)
        if self.num_servers is None:
            self.num_servers = len(num_servers)
        if self.init_start is None:
            self.init_start = datetime.datetime.fromtimestamp(min_timestamp)
        if self.init_end is None:
            self.init_end = datetime.datetime.fromtimestamp(max_timestamp)

        # then initialize all datasets
        for dataset_name, columns in datasets.items():
            tokio.debug.debug_print("Initializing %s with %d columns between %s and %s" %
                                    (dataset_name, len(columns), self.init_start, self.init_end))
            self.init_dataset(
                dataset_name=dataset_name,
                columns=list(columns))

    def finalize(self):
        """Convert datasets to deltas where necessary and tack on metadata

        Perform a few finishing actions to all datasets contained in self after
        they have been populated.  Such actions are configured entirely in
        self.config and require no external input.
        """

        # convert CPU loads to percents
        for dataset_name in self.keys():
            if dataset_name.endswith('cpuuser') or dataset_name.endswith('cpusys'):
                self[dataset_name].dataset *= 100.0

        self.set_timeseries_metadata(self.keys())

    def set_timeseries_metadata(self, dataset_names):
        """Set metadata constants (version, units, etc) on datasets and groups

        Args:
            dataset_names (list of str): datasets whose metadata should be set
        """
        for dataset_name in dataset_names:
            if dataset_name in self:
                self[dataset_name].dataset_metadata.update({
                    'version': SCHEMA_VERSION,
                    'units': UNITS_MAP.get(dataset_name, {}).get('units', "UNKNOWN"),
                })
                self[dataset_name].group_metadata.update({'source': 'mmperfmon'})

    def archive(self, mmpm):
        """Extracts and encode data from an Mmperfmon object

        Uses the mmperfmon connector to populate one or more TimeSeries objects.

        Args:
            mmpm (tokio.connectors.mmperfmon.Mmperfmon): Instance of the
                mmperfmon connector class containing all of the data to be
                archived
        """
        self.init_datasets(mmpm)

        for timestamp_int, fqhosts in mmpm.items():
            for fqhost, counters in fqhosts.items():
                server_type = self.server_type(fqhost)
                for counter, value in counters.items():
                    # if this counter has many instances (e.g., non-normalized)
                    if isinstance(value, dict):
                        for instance, actual_value in value.items():
                            column = fqhost + ":" + instance

                            # infer dataset name from counter name
                            lun_type = self.lun_type(instance)

                            # without knowing the LUN type, we cannot safely do anything
                            if not lun_type:
                                #tokio.debug.debug_print("instance of %s is %s" % (counter, instance))
                                # currently this just drops all the network counters for loopback
                                continue

                            dataset_name = "%ss/%s" % (lun_type, COUNTER_MAP.get(counter, counter))

                            # insert element
                            self[dataset_name].insert_element(
                                timestamp=datetime.datetime.fromtimestamp(int(timestamp_int)),
                                column_name=column,
                                value=actual_value,
                                align='r')
                    else:
                        canonical_counter = COUNTER_MAP.get(counter, counter)
                        dataset_name = "%ss/%s" % (server_type, canonical_counter)
                        # insert element
                        self[dataset_name].insert_element(
                            timestamp=datetime.datetime.fromtimestamp(int(timestamp_int)),
                            column_name=fqhost,
                            value=value,
                            align='r')

                        # little hacky bits to patch together missing datasets
                        if canonical_counter == "cpuuser":
                            dataset_name = "%ss/cpuload" % server_type
                            self[dataset_name].insert_element(
                                timestamp=datetime.datetime.fromtimestamp(int(timestamp_int)),
                                column_name=fqhost,
                                value=value,
                                reducer=operator.add,
                                align='r')

        tokio.debug.debug_print("Found %d hosts" % self.num_servers)
        tokio.debug.debug_print("Found %d timestamps" % len(set(list(mmpm.keys()))))

    def lun_type(self, lun_name):
        """Infers the dataset name to which a LUN should belong

        Returns the dataset name in which a given GPFS LUN name belongs.  This is
        required for block-based file systems in which servers serve both data and
        metadata.

        This function relies on tokio.config.CONFIG['mmperfmon_lun_map'].

        Args:
            lun_name (str): The name of a LUN

        Returns:
            str: The name of a dataset in which `lun_name` should be filed.
        """
        if lun_name in self.lun_types:
            return self.lun_types[lun_name]

        if not self.lun_map:
            # without a LUN map, no way to tell if a LUN is a data or metadata target
            if "mmperfmon_lun_map" not in tokio.config.CONFIG:
                tokio.debug.debug_print("No mmperfmon_lun_map in tokio.config.CONFIG")
                return ""

            self.lun_map = {}
            for regex_str, ltype in tokio.config.CONFIG["mmperfmon_lun_map"].items():
                self.lun_map[re.compile(regex_str)] = ltype

        for regex, ltype in self.lun_map.items():
            if regex.match(lun_name):
                self.lun_types[lun_name] = ltype
                return ltype

        return ""

    def server_type(self, server_name):
        """Infers the type of server (data or metadata) from its name

        Returns the type of server that `server_name` is.  This relies on
        tokio.config.CONFIG['mmperfmon_md_servers'] which encodes a regex that
        matches metadata server names.

        This method only makes sense for GPFS clusters that have distinct
        metadata servers.

        Args:
            server_name (str): Name of the server

        Returns:
            str: "mdserver" or "dataserver"
        """
        if server_name in self.server_types:
            return self.server_types[server_name]

        if not self.server_map:
            # without a LUN map, no way to tell if a LUN is a data or metadata target
            if "mmperfmon_server_map" not in tokio.config.CONFIG:
                return "dataserver"

            self.server_map = {}
            for regex_str, stype in tokio.config.CONFIG["mmperfmon_server_map"].items():
                self.server_map[re.compile(regex_str)] = stype

        for regex, stype in self.server_map.items():
            if regex.match(server_name):
                self.server_types[server_name] = stype
                return stype

        self.server_types[server_name] = "dataserver"
        return self.server_types[server_name]

def init_hdf5_file(datasets, init_start, init_end, hdf5_file):
    """Creates HDF5 datasets within a file based on TimeSeries objects

    Idempotently ensures that `hdf5_file` contains a dataset corresponding to
    each tokio.timeseries.TimeSeries object contained in the `datasets` object.

    Args:
        datasets (Archiver): Dictionary keyed by dataset name and whose values
            are tokio.timeseries.TimeSeries objects.  One HDF5 dataset will be
            created for each TimeSeries object.
        init_start (datetime.datetime): If a dataset does not already exist
            within the HDF5 file, create it using this as a lower bound for
            the timesteps, inclusive
        init_end (datetime.datetime): If a dataset does not already exist within
            the HDF5 file, create one using this as the upper bound for the
            timesteps, exclusive
        hdf5_file (str): Path to the HDF5 file in which datasets should be
            initialized
    """
    schema = tokio.connectors.hdf5.SCHEMA.get(SCHEMA_VERSION)
    for dataset_name, dataset in datasets.items():
        hdf5_dataset_name = schema.get(dataset_name)
        if hdf5_dataset_name is None:
            if '/_' not in dataset_name:
                warnings.warn("Dataset key %s is not in schema" % dataset_name)
            continue

        if hdf5_dataset_name not in hdf5_file:
            # attempt to convert dataset into a timeseries
            timeseries = hdf5_file.to_timeseries(dataset_name=hdf5_dataset_name)

            # if dataset -> timeseries failed, create and commit a new, empty timeseries
            if timeseries is None:
                timeseries = tokio.timeseries.TimeSeries(dataset_name=hdf5_dataset_name,
                                                         start=init_start,
                                                         end=init_end,
                                                         timestep=dataset.timestep,
                                                         num_columns=dataset.dataset.shape[1])
                hdf5_file.commit_timeseries(timeseries=timeseries)
            print("Initialized %s in %s with size %s" % (
                hdf5_dataset_name,
                hdf5_file.name,
                timeseries.dataset.shape))

def archive_mmperfmon(init_start, init_end, timestep, num_luns, num_servers, output_file, input_files):
    """Retrieves remote data and stores it in TOKIO time series format

    Given a start and end time, retrieves all of the relevant contents of a
    remote data source and encodes them in the TOKIO time series HDF5 data
    format.

    Args:
        init_start (datetime.datetime): The first timestamp to be included in
            the HDF5 file
        init_end (datetime.datetime): The timestamp following the last timestamp
            to be included in the HDF5 file.
        timestep (int or None): Number of seconds between successive entries in
            the HDF5 file to be created.  If None, autodetect.
        num_luns (int or None): Number of LUNs expected to appear in mmperfmon
            outputs.  If None, autodetect.
        num_servers (int or None): Number of NSD servers expected to appear in
            mmperfmon outputs.  If None, autodetect.
        output_file (str): Path to the file to be created.
        input_files (list of str): List of paths to input files from which
            mmperfmon connectors should be instantiated.
    """
    mmpm = None
    for input_file in input_files:
        if mmpm is None:
            mmpm = tokio.connectors.mmperfmon.Mmperfmon.from_file(input_file)
        else:
            mmpm.load(input_file)

    datasets = Archiver(
        init_start=init_start,
        init_end=init_end,
        timestep=timestep,
        num_luns=num_luns,
        num_servers=num_servers)

    datasets.archive(mmpm)

    datasets.finalize()

    with tokio.connectors.hdf5.Hdf5(output_file, libver='latest') as hdf5_file:
        hdf5_file.attrs['version'] = SCHEMA_VERSION

        init_hdf5_file(datasets, datasets.init_start, datasets.init_end, hdf5_file)

        for dataset in datasets.values():
            print("Writing out %s" % dataset.dataset_name)
            hdf5_file.commit_timeseries(dataset)

    tokio.debug.debug_print("Wrote output to %s" % output_file)

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", type=str, default='output.hdf5',
                        help="output file (default: output.hdf5)")
    parser.add_argument('--init-start', type=str, default=None,
                        help='first timestamp (inclusive) when creating new output file,' +
                        ' in %s format (default: same as start)' % DATE_FMT_PRINT)
    parser.add_argument('--init-end', type=str, default=None,
                        help='final timestamp (exclusive) when creating new output file,' +
                        ' in %s format (default: same as end)' % DATE_FMT_PRINT)
    parser.add_argument('--debug', action='store_true', help="produce debug messages")
    parser.add_argument('--timestep', type=int, default=60,
                        help='collection frequency, in seconds (default: 60)')
    parser.add_argument('--num-luns', type=int, default=None,
                        help="number of LUNs (default: autodetect)")
    parser.add_argument('--num-servers', type=int, default=None,
                        help="number of NSD servers (default: autodetect)")
    parser.add_argument("--files", type=str, nargs="*", help="path to mmperfmon output file to "
                        + "use instead of query_start/query_end")
    parser.add_argument("--filesystem", type=str, required=True, help='file system to archive')
    parser.add_argument("query_start", type=str,
                        help="start time of query in %s format" % DATE_FMT)
    parser.add_argument("query_end", type=str,
                        help="end time of query in %s format" % DATE_FMT)
    args = parser.parse_args(argv)

    if args.debug:
        tokio.debug.DEBUG = True

    if (not args.files and not args.query_start and not args.query_end) or \
       (args.files and args.query_start and args.query_end):
        parser.error("Must specify either --file or query_start and query_end")

    # Convert CLI options into datetime
    init_start = None
    init_end = None
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
        parser.error("Start and end times must be in format %s\n" % DATE_FMT)
        raise

    # Basic input bounds checking
    if query_start >= query_end:
        parser.error('query_start >= query_end')
    elif init_start >= init_end:
        parser.error('init_start >= init_end')
    elif args.timestep < 1:
        parser.error('--timestep must be > 0')

    files = args.files
    if not files:
        files = tokio.tools.nersc_mmperfmon.enumerate_mmperfmon_txt(
            fsname=args.filesystem,
            datetime_start=query_start,
            datetime_end=query_end)

    if not files:
        raise RuntimeError("No input files match query range")
    if args.debug:
        print("Loading the following files:\n" + "\n  ".join(files))

    archive_mmperfmon(
        init_start=init_start,
        init_end=init_end,
        timestep=args.timestep,
        num_luns=args.num_luns,
        num_servers=args.num_servers,
        output_file=args.output,
        input_files=files)
