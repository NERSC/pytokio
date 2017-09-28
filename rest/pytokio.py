#!/usr/bin/env python
"""
Wrappers around the pytokio API that provide a REST interface into some
connectors.
"""

import json
import bottle

@bottle.route('/v1/<host:re:franklin|hopper|edison|cori>/<begin:re:\d{10}>/<end:re:\d{10}>/<data:re:bulk|metadata>/')
@bottle.route('/v1/<host:re:franklin|hopper|edison|cori>/<begin:re:\d{10}>/<end:re:\d{10}>/<data:re:bulk|metadata>')
def base_route(host, begin, end, data):
    """
    Demonstrate basic bottle integration
    """
    bottle.response.content_type = 'application/json'
    bottle.response.headers['Access-Control-Allow-Origin'] = '*'
    response = {
        'host': host,
        'begin': begin,
        'end': end,
        'data': data,
    }
    return json.dumps({'status': 'success', 'response': response})

bottle.debug(True)

# Do NOT use bottle.run() with mod_wsgi
if __name__ == '__main__':
    bottle.run(host='localhost', port=18880, reloader=True)
else:
    application = bottle.default_app()
