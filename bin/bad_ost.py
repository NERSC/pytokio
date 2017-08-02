#!/usr/bin/env python

import sys
import json
import tokio.connectors.darshan

d = tokio.connectors.darshan.Darshan( sys.argv[1] )
d.darshan_parser_base()

# print json.dumps(d, indent=4, sort_keys=True)

if 'lustre' not in d['counters']:
    raise Exception("Darshan log does not contain Lustre module data")


results = {}

for record_name, posix_rank_data in d['counters']['posix'].iteritems():
    if record_name in ('_perf', '_total'):
        continue

    (_,counters), = posix_rank_data.items()

    if record_name not in d['counters']['lustre'].keys():
        print record_name, "not in lustre records"
        continue
        
    io_time = counters['F_WRITE_TIME'] + counters['F_READ_TIME'] + counters['F_META_TIME']
    io_volume = counters['BYTES_READ'] + counters['BYTES_WRITTEN']
    io_perf = io_volume / io_time
    
    (_, lustre_data), = d['counters']['lustre'][record_name].items()

    k = lustre_data['OST_ID_0']
    v = io_perf

    results[k] = v

print "%12s %s" % ("MiB/s", "OBD Index")
for obd_idx in sorted(results.keys(), key=results.get):
    print "%12.2f %s" % (results[obd_idx] * 2.0**(-20.0), obd_idx)
