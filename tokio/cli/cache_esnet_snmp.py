"""
Provide a CLI interface for :meth:`tokio.connectors.esnet_snmp.EsnetSnmp.to_dataframe`
and :meth:`tokio.connectors.esnet_snmp.EsnetSnmp.save_cache` methods.
"""

import sys
import datetime
import argparse
import tokio.connectors.esnet_snmp
import tokio.config

DATE_FMT = "%Y-%m-%dT%H:%M:%S"
DATE_FMT_PRINT = "YYYY-MM-DDTHH:MM:SS"

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", type=str,
                        help="start time of query in %s format" % DATE_FMT_PRINT)
    parser.add_argument("-e", "--end", type=str,
                        help="end time of query in %s format" % DATE_FMT_PRINT)
    parser.add_argument("-j", "--json", action="store_true", default=True,
                        help="return output in JSON format")
    parser.add_argument("-c", "--csv", action="store_true",
                        help="return output in CSV format")
#   parser.add_argument('--debug', action='store_true',
#                       help="produce debug messages")
    parser.add_argument("-i", "--input", type=str, default=None,
                        help="read input from this JSON instead of accessing REST API")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="output file")
    parser.add_argument("endpoints", type=str,
                        help='endpoint(s) to process, either a center name'
                        + ' ("nersc") or comma-separated list of format'
                        + ' "endpoint0:if0,endpoint1:if1,..." etc')
    args = parser.parse_args(argv)

#   if args.debug:
#       tokio.DEBUG = True

    # Parse endpoints and interfaces
    if ':' not in args.endpoints:
        query_args = tokio.config.CONFIG.get('esnet_snmp_interfaces', {}).get(args.endpoints, {}).copy()
    else:
        query_args = {}
        for kvpair in args.endpoints.split(","):
            key, value = kvpair.split(":", 1)
            if key not in query_args:
                query_args[key] = []
            query_args[key].append(value)

    if not query_args:
        errstr = "Invalid endpoint specification.\n\n"
        errstr += "Valid endpoints:" + "\n  ".join(tokio.config.CONFIG.get('esnet_snmp_interfaces', {}).keys())
        errstr += "\n\nor endpoint:interface[,endpoint:interface[,endpoint:interface]]"
        sys.stderr.write(errstr + "\n")
        raise ValueError("Invalid endpoint specification")

    if args.end and not args.start:
        parser.error("--start must be specified with --end")
    try:
        if args.start:
            start = datetime.datetime.strptime(args.start, DATE_FMT)
        if args.end:
            end = datetime.datetime.strptime(args.end, DATE_FMT)
    except ValueError:
        raise ValueError("Start and end times must be in format %s\n" % DATE_FMT_PRINT)

    if not args.start:
        start = datetime.datetime.now() - datetime.timedelta(hours=1)
    if not args.end:
        end = start + datetime.timedelta(hours=1)

    # Basic input bounds checking
    if start >= end:
        raise ValueError('--start >= --end')

    if args.input:
        esnetdata = tokio.connectors.esnet_snmp.EsnetSnmp(start=start, end=end, input_file=args.input)
    else:
        # Initialize query interface
        esnetdata = tokio.connectors.esnet_snmp.EsnetSnmp(start=start, end=end)

        # Query each endpoint:interface specified
        for endpoint, interfaces in query_args.items():
            for interface in interfaces:
                ret = esnetdata.get_interface_counters(endpoint=endpoint, interface=interface,
                                                       direction="in",
                                                       agg_func='average')
                ret = esnetdata.get_interface_counters(endpoint=endpoint,
                                                       interface=interface,
                                                       direction="out",
                                                       agg_func='average')

    # Serialize the object
    cache_file = args.output
    if cache_file is not None:
        print("Caching to %s" % cache_file)

    if args.csv:
        if cache_file is None:
            print(esnetdata.to_dataframe().to_csv())
        else:
            esnetdata.to_dataframe().to_csv(cache_file)
    elif args.json:
        if cache_file is not None:
            esnetdata.save_cache(cache_file)
        else:
            esnetdata.save_cache(cache_file, indent=4, sort_keys=True)
            sys.stdout.write("\n")
#   else:
#       # should never be encountered; default is args.json = True
