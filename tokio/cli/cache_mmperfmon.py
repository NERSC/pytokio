"""
Provide a CLI interface for :meth:`tokio.connectors.mmperfmon.Mmperfmon.to_dataframe`
and :meth:`tokio.connectors.mmperfmon.Mmperfmon.save_cache` methods.
"""

import json
import argparse
import tokio.connectors.mmperfmon

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, nargs="+", help="mmperfmon text output to process")
    parser.add_argument("-j", "--json", action="store_true", default=True,
                        help="return output in JSON format")
    parser.add_argument("-c", "--csv", action="store_true", help="return output in CSV format")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    parser.add_argument("--host", type=str, default=None, help="hostname to output")
    parser.add_argument("--metric", type=str, default=None, help="metric to output")
    args = parser.parse_args(argv)

    if args.csv:
        if (not args.host and not args.metric) or (args.host and args.metric):
            parser.error("Must specify one --host or --metric with --csv")

    # Read from a cache file
    mmperfmon = tokio.connectors.mmperfmon.Mmperfmon(args.input[0])
    if len(args.input) > 1:
        for input_filename in args.input[1:]:
            mmperfmon.load_cache(input_filename)

    # Serialize the object
    cache_file = args.output
    if cache_file is not None:
        print("Caching to %s" % cache_file)

    if args.csv:
        if cache_file is None:
            print(mmperfmon.to_dataframe(by_host=args.host, by_metric=args.metric).to_csv())
        else:
            mmperfmon.to_dataframe(by_host=args.host, by_metric=args.metric).to_csv(cache_file)
    elif args.json:
        mmperfmon.save_cache(cache_file)
    else:
        raise Exception("No output format specified")
