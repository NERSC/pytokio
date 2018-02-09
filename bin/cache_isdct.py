#!/usr/bin/env python
"""
Parse and cache an ISDCT dump to simply reanalysis and sharing its data.
"""

import argparse
import tokio.connectors.nersc_isdct

def main(argv=None):
    """
    CLI wrapper around the NerscIsdct object's I/O methods
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("isdctfile", type=str, help="darshan logs to process")
    parser.add_argument("-j", "--json", action="store_true", default=True,
                        help="return output in JSON format")
    parser.add_argument("-c", "--csv", action="store_true", help="return output in CSV format")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    args = parser.parse_args(argv)

    # Read from a cache file
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(args.isdctfile)

    # Serialize the object
    cache_file = args.output
    if cache_file is not None:
        print "Caching to %s" % cache_file

    if args.csv:
        if cache_file is None:
            print isdct_data.to_dataframe().to_csv()
        else:
            isdct_data.to_dataframe().to_csv(cache_file)
    elif args.json:
        isdct_data.save_cache(cache_file)
    else:
        raise Exception("No output format specified")

if __name__ == "__main__":
    main()
