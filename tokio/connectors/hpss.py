"""Connect to various outputs made available by HPSS
"""

import re
import copy
import datetime
from tokio.connectors.common import SubprocessOutputDict

REX_HEADING_LINE = re.compile(r"^[= ]+$")
REX_EMPTY_LINE = re.compile(r"^\s*$")
REX_TIMEDELTA = re.compile(r"^(\d+)-(\d+):(\d+):(\d+)$")

FLOAT_KEYS = set([
    'io_gb',
    'write_gb',
    'read_gb',
    'copy_gb',
    'mig (gb)',
    'purge(gb)',
    'lock%'
])
INT_KEYS = set([
    'users',
    'ops',
    'w_ops',
    'r_ops',
    'c_ops',
    'migfiles',
    'purfiles',
    'count',
    'cleans',
    'locks',
    'mounts',
])
DELTIM_KEYS = set([
    'migtime',
    'purgetime',
    'availtime',
    'locktime',
    'mounttime',
])

REKEY_TABLES = {
    'io totals by client application': 'client',
    'io totals by client host': 'host',
    'io totals by hpss client gateway (ui) host': 'host',
#   'largest users': 'user', # degenerate users will occur if one user uses multiple client apps
    'migration purge report': 'sc',
#   'tape drive report': 'drivetyp',
}

class HpssDailyReport(SubprocessOutputDict):
    """Representation for the daily report that HPSS can generate
    """
    def __init__(self, *args, **kwargs):
        super(HpssDailyReport, self).__init__(*args, **kwargs)
        self.date = None
        self.load()

    def load_str(self, input_str):
        """Parse the HPSS daily report text
        """
        lines = input_str.splitlines()
        num_lines = len(lines)
        start_line = 0

        # Look for the header for the whole report to get the report date
        for start_line, line in enumerate(lines):
            if line.startswith("HPSS Report for Date"):
                self.date = datetime.datetime.strptime(line.split()[-1], "%Y-%m-%d")
                break
        if not self.date:
            raise IndexError("No report date found")

        # Try to find tables encoded in the remainder of the report
        while start_line < num_lines:
            parsed_table, finish_line = _parse_section(lines, start_line)
            if finish_line != start_line and 'records' in parsed_table:
                if parsed_table['system'] not in self:
                    self.__setitem__(parsed_table['system'], {})

                # convert a list of records into a dict of indices
                if parsed_table['title'] in REKEY_TABLES:
                    parsed_table = _rekey_table(parsed_table,
                                                key=REKEY_TABLES[parsed_table['title']])

                self[parsed_table['system']][parsed_table['title']] = parsed_table['records']
            start_line += 1

def _parse_section(lines, start_line=0):
    """Parse a single table of the HPSS daily report

    Converts a table from the HPSS daily report into a dictionary.  For example
    an example table may appear as::

        Archive : IO Totals by HPSS Client Gateway (UI) Host
        Host             Users      IO_GB       Ops
        ===============  =====  =========  ========
        heart               53   148740.6     27991
        dtn11                5    29538.6      1694
        Total               58   178279.2     29685
        HPSS ACCOUNTING:         224962.6

    which will return a dict of form::

        {
            "system": "archive",
            "title": "io totals by hpss client gateway (ui) host",
            "records": {
                "heart": {
                    "io_gb": "148740.6",
                    "ops": "27991",
                    "users": "53",
                },
                "dtn11": {
                    "io_gb": "29538.6",
                    "ops": "1694",
                    "users": "5",
                },
                "total": {
                    "io_gb": "178279.2",
                    "ops": "29685",
                    "users": "58",
                }
            ]
        }

    This function is robust to invalid data, and any lines that do not appear to
    be a valid table will be treated as the end of the table.

    Args:
        lines (list of str): Text of the HPSS report
        start_line (int): Index of ``lines`` defined such that

          * ``lines[start_line]`` is the table title
          * ``lines[start_line + 1]`` is the table heading row
          * ``lines[start_line + 2]`` is the line separating the table heading and
            the first row of data
          * ``lines[start_line + 3:]`` are the rows of the table

    Returns:
        tuple:
          Tuple of (dict, int) where

          * dict contains the parsed contents of the table
          * int is the index of the last line of the table + 1
    """
    results = {}

    # Skip any initial whitespace
    num_lines = len(lines)
    while start_line < num_lines and REX_EMPTY_LINE.match(lines[start_line]):
        start_line += 1

    # Did we skip past the end of the input data?
    if start_line >= num_lines:
        return results, start_line

    # Parse table title (if available).  This can pick up times (0:00:00) so do
    # not treat system, title as legitimate values until we also identify the
    # line below column headings.
    if ':' not in lines[start_line]:
        return results, start_line
    else:
        system, title = lines[start_line].split(':', 1)

    # Determine column delimiters
    separator_line = lines[start_line + 2]
    col_extents = _find_columns(separator_line)
    if len(col_extents) == 0:
        return results, start_line

    # At this point, we are reasonably confident we have found a table.
    # Populate results so that this function returns some indicator of
    # success.
    results['system'] = system.strip().lower()
    results['title'] = title.strip().lower()

    # Determine column headers
    heading_line = lines[start_line + 1]
    headings = []
    for start_pos, str_len in col_extents:
        headings.append(heading_line[start_pos:start_pos + str_len].strip())

    records = []
    index = 0
    for index, line in enumerate(lines[start_line + 3:]):
        # check for end of record (empty line)
        if REX_EMPTY_LINE.match(line):
            # an empty line denotes end of table
            break
        elif len(line) < (col_extents[-1][0] + col_extents[-1][1] - 1):
            # line is malformed; this happens for table summaries
            break

        record = {}
        for heading_idx, (start_pos, str_len) in enumerate(col_extents):
            col_name = headings[heading_idx].lower()
            col_val = line[start_pos:start_pos + str_len].lower().strip()
            if col_name in FLOAT_KEYS:
                record[col_name] = float(col_val)
            elif col_name in INT_KEYS:
                record[col_name] = int(col_val)
            elif col_name in DELTIM_KEYS:
                record[col_name] = col_val
                record[col_name + "secs"] = _hpss_timedelta_to_secs(col_val)
            else:
                record[col_name] = col_val
        records.append(record)

    if records:
        results['records'] = records

    return (results, index + 1)

def _find_columns(line, sep="=", gap=' ', strict=False):
    """Determine the column start/end positions for a header line separator

    Takes a line separator such as the one denoted below:

        Host             Users      IO_GB
        ===============  =====  =========
        heart               53   148740.6

    and returns a tuple of (start index, end index) values that can be used to
    slice table rows into column entries.

    Args:
        line (str): Text comprised of separator characters and spaces that
            define the extents of columns
        sep (str): The character used to draw the column lines
        gap (str): The character separating ``sep`` characters
        strict (bool): If true, restrict column extents to only include sep
            characters and not the spaces that follow them.

    Returns:
        list of tuples:
    """

    columns = []

    # if line is not comprised exclusively of separators and gaps, it is not a
    # valid heading line
    if line.replace(sep, 'X').replace(gap, 'X').strip('X') != "":
        return columns

    if strict:
        col_start = None
    else:
        col_start = 0

    for index, char in enumerate(line):
        if strict:
            # col_start == None == looking for start of a new column
            if col_start is None and char == sep:
                col_start = index
            # if this is the end of an inter-column gap
            elif index > 0 and char == gap and line[index - 1] == sep:
                columns.append((col_start, index - col_start))
                col_start = None
        else:
            # if this is the end of an inter-column gap
            if index > 0 and char == gap and line[index - 1] == sep:
                columns.append((col_start, index - col_start))
                col_start = index

    if line and line[-1] == sep and col_start is not None:
        columns.append((col_start, len(line) - col_start))

    return columns

def _rekey_table(table, key):
    """Converts a list of records into a dict of records

    Converts a table of records as returned by _parse_section() of the form::

        {
            "records": [
                {
                    "host": "heart",
                    "io_gb": "148740.6",
                    "ops": "27991",
                    "users": "53",
                },
                ...
            ]
        }

    Into a table of key-value pairs the form::

        {
            "records": {
                "heart": {
                    "io_gb": "148740.6",
                    "ops": "27991",
                    "users": "53",
                },
                ...
            }
        }

    Does not handle degenerate keys when re-keying, so only some tables with a
    uniquely identifying key can be rekeyed.

    Args:
        table (dict): Output of the _parse_section() function
        key (str): Key to pull out of each element of table['records'] to use as
            the key for each record

    Returns:
        dict: Table with records expressed as key-value pairs instead of a list
    """
    new_table = copy.deepcopy(table)
    new_records = {}

    for record in new_table['records']:
        new_key = record.pop(key)
        if new_key in new_records:
            raise KeyError("Degenerate key %s=%s" % (key, new_key))
        new_records[new_key] = record

    new_table['records'] = new_records

    return new_table

def _hpss_timedelta_to_secs(timedelta_str):
    """Convert HPSS-encoded timedelta string into seconds

    Args:
        timedelta_str (str): String in form d-HH:MM:SS where d is the number of
            days, HH is hours, MM minutes, and SS seconds

    Returns:
        int: number of seconds represented by timedelta_str
    """

    match = REX_TIMEDELTA.match(timedelta_str)
    if match:
        seconds = int(match.group(1)) * 86400
        seconds += int(match.group(2)) * 3600
        seconds += int(match.group(3)) * 60
        seconds += int(match.group(4))
    else:
        seconds = -1

    return seconds
