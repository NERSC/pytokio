"""Archive NERSC Globus logs into SQLite
"""

import warnings
import argparse
import datetime
import sys
import sqlite3
import tokio.debug
import tokio.connectors.nersc_globuslogs

DATE_FMT = "%Y-%m-%dT%H:%M:%S"

SCHEMA = [
    {"field": "timestamp", "source": "@timestamp"},
    {"source": "BLOCK", "type": "integer"},
    {"source": "BUFFER", "type": "integer"},
    {"source": "CODE", "type": "integer"},
    {"source": "DATE"},
    {"source": "DEST"},
    {"source": "DESTIP"},
    {"source": "FILE"},
    {"source": "HOST"},
    {"source": "NBYTES", "type": "integer"},
    {"source": "START"},
    {"source": "STREAMS", "type": "integer"},
    {"source": "STRIPES", "type": "integer"},
    {"source": "TASKID"},
    {"source": "TYPE"},
    {"source": "USER"},
    {"source": "VOLUME"},
    {"source": "bandwidth_mbps", "type": "real"},
    {"source": "duration", "type": "real"},
    {"source": "start_date"},
    {"source": "end_date"},
    {"field": "loghost", "source": "loghost"},
]

def query_to_sqlite(esdb, start, end, conn, table):
    """Stores the results of an esdb query as SQLite

    Queries Elasticsearch for all Globus transfers that occurred during a
    certain time and saves them into an SQLite database.

    Args:
        esdb (tokio.connectors.nersc_globuslogs.NerscGlobusLogs): Connection to
            the Elasticsearch service that contains an index of Globus transfer
            logs
        start (datetime.datetime): Query for logs that ended at or after this
            time
        end (datetime.datetime): Query for logs that started at or before this
            time
        conn (sqlite3.Connection): Connection to SQLite database into which
            the logs should be inserted
        table (str): Name of table into which logs should be inserted
    """

    esdb.query(start, end)

    query = "CREATE TABLE IF NOT EXISTS %s (\n" % table
    field_sources = []
    field_names = []
    field_validate = []
    for index, field in enumerate(SCHEMA):
        field_source = field.get('source')
        field_name = field.get('field', field_source.lower())
        field_type = field.get('type', 'char').upper()
        if index == len(SCHEMA) - 1:
            query += "    %s %s\n" % (field_name, field_type)
        else:
            query += "    %s %s,\n" % (field_name, field_type)
        field_sources.append(field_source)
        field_names.append(field_name)
        if field_type == 'INTEGER':
            field_validate.append(lambda x: int(x))
        elif field_type == 'REAL':
            field_validate.append(lambda x: float(x))
        else:
            field_validate.append(lambda x: x)
    query += ")"

    cursor = conn.cursor()
    tokio.debug.debug_print(query)
    cursor.execute(query)

    query = "INSERT OR IGNORE INTO %s (" % table
    query += ", ".join(field_names)
    query += ") VALUES (" + ", ".join(["?"] * len(field_names))
    query += ")"
    tokio.debug.debug_print(query)
    inserts = []
    for page in esdb.scroll_pages:
        for record in page:
            try:
                inserts.append(
                    tuple([field_validate[i](record['_source'].get(x)) for i, x in enumerate(field_sources)]))
            except TypeError:
                warnings.warn("Malformed source record; skipping")
    
    cursor.executemany(query, inserts)

    cursor.close()
    conn.commit()

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
    parser.add_argument('--debug', action='store_true',
                        help="produce debug messages")
    parser.add_argument('--timeout', type=int, default=30,
                        help='ElasticSearch timeout time (default: 30)')
    parser.add_argument('--input', type=str, default=None,
                        help="use cached output from previous ES query")
    parser.add_argument("-o", "--output", type=str, default='globuslogs.db',
                        help="output file (default: globuslogs.db)")
    parser.add_argument("-t", "--table", type=str, default="transfers",
                        help="name of table to populate (default: transfers)")
    parser.add_argument('-h', '--host', type=str, default="localhost",
                        help="hostname of ElasticSearch endpoint (default: localhost)")
    parser.add_argument('-p', '--port', type=int, default=9200,
                        help="port of ElasticSearch endpoint (default: 9200)")
    parser.add_argument('-i', '--index', type=str, default='dtn-dtn-log*',
                        help='ElasticSearch index to query (default:dtn-dtn-log*)')
    args = parser.parse_args(argv)

    if args.debug:
        tokio.DEBUG = True

    # Convert CLI options into datetime
    try:
        query_start = datetime.datetime.strptime(args.query_start, DATE_FMT)
        query_end = datetime.datetime.strptime(args.query_end, DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % DATE_FMT)
        raise

    # Basic input bounds checking
    if query_start >= query_end:
        raise Exception('query_start >= query_end')

    # Read input from a cached json file (generated previously) or by querying
    # Elasticsearch directly
    if args.input is None:
        ### Try to connect
        esdb = tokio.connectors.nersc_globuslogs.NerscGlobusLogs(
            host=args.host,
            port=args.port,
            index=args.index,
            timeout=args.timeout)

        tokio.debug.debug_print("Loaded results from %s:%s" % (args.host, args.port))
    else:
        esdb = tokio.connectors.nersc_globuslogs.NerscGlobusLogs.from_cache(args.input)
        tokio.debug.debug_print("Loaded results from %s" % args.input)

    print("Writing output to %s" % args.output)
    conn = sqlite3.connect(args.output)
    query_to_sqlite(esdb, query_start, query_end, conn, args.table)
    conn.close()
