pytokio REST API
================================================================================

This contains a Flask application which provides REST-based access to some of
the data and tools that pytokio supports.  To get started, run the
`pytokio_rest.py` script to launch the endpoint and then play with it:

    curl -L -H "Accept: application/json" http://localhost:18880/hdf5/cscratch/mdscpu

where

    * `-L` is necessary to follow redirects
    * `-H "Accept: application/json"` indicates that you want json, not
      HTML-formatted output (which is curl's default)
