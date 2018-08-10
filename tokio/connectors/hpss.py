"""Connect to various outputs made available by HPSS
"""

import re
from tokio.connectors.common import SubprocessOutputDict

REX_HEADING_LINE = re.compile(r"^[= ]+$")
REX_EMPTY_LINE = re.compile(r"^\s*$")

class HpssDailyReport(SubprocessOutputDict):
    """Representation for the daily report that HPSS can generate
    """
    def __init__(self, *args, **kwargs):
        super(HpssDailyReport, self).__init__(*args, **kwargs)

    def __repr__(self):
        """
        """

    def load_str(self, input_str):
        """Parse the HPSS daily report text
        """

def _parse_section(lines, start_line=0):
    """Parse a single table of the HPSS daily report

    Args:
        lines (list of str): Text of the HPSS report
        start_line (int): Index of ``lines`` defined such that
          * lines[start_line] is the table title
          * lines[start_line + 1] is the table heading row
          * lines[start_line + 2] is the line separating the table heading and
            the first row of data
          * lines[start_line + 3:] are the rows of the table

    Returns:
        tuple of (dict, int) where
          * str
          * dict contains the parsed contents of the table
          * int is the index of the last line of the table + 1
    """
    # Skip any initial whitespace
    while REX_EMPTY_LINE.match(lines[start_line]):
        start_line += 1

    # Parse table title
    system, title = lines[start_line].split(':', 1)

    # Determine column delimiters
    separator_line = lines[start_line + 2]
    col_extents = _find_columns(separator_line)

    # Determine column headers
    heading_line = lines[start_line + 1]
    headings = []
    for start_pos, str_len in col_extents:
        headings.append(heading_line[start_pos:start_pos + str_len].strip())

    records = []
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
            record[col_name] = col_val
        records.append(record)

    results = {
        'system': system,
        'title': title,
        'records': records,
    }

    return (records, index + 1)

def _find_columns(line, sep="=", strict=False):
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
        strict (bool): If true, restrict column extents to only include sep
            characters and not the spaces that follow them.

    Returns:
        list of tuples:
    """
    columns = []
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
            elif index > 0 and char != sep and line[index - 1] == sep:
                columns.append((col_start, index - col_start))
                col_start = None
        else:
            # if this is the end of an inter-column gap
            if index > 0 and char != sep and line[index - 1] == sep:
                columns.append((col_start, index - col_start))
                col_start = index

    if line and line[-1] == sep and col_start is not None:
        columns.append((col_start, len(line) - col_start))

    return columns
