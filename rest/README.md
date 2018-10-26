pytokio REST API
================================================================================

This contains a Flask application which provides REST-based access to some of
the data and tools that pytokio supports.  To get started, run the
`pytokio_rest.py` script to launch the endpoint and then play with it:

    $ curl -L -H "Accept: application/json" http://localhost:18880/hdf5
    $ curl -L -H "Accept: application/json" http://localhost:18880/hdf5/cscratch
    $ curl -L -H "Accept: application/json" http://localhost:18880/hdf5/cscratch/mdservers
    $ curl -L -H "Accept: application/json" http://localhost:18880/hdf5/cscratch/mdservers/cpuload

where

    * `-L` is necessary to follow redirects
    * `-H "Accept: application/json"` indicates that you want json, not
      HTML-formatted output (which is curl's default)

Known Issues
--------------------------------------------------------------------------------

Tests are not passing.

The "hdf5" top-level tool is a non-intuitive name.  Should be something like
"fsload" instead.

Invalid data is not handled well:

    [2018-10-25 22:07:38,584] ERROR in app: Exception on /hdf5/cscratch/mdservers/cpuload [GET]
    Traceback (most recent call last):
      File "/Users/glock/.apps/anaconda3/lib/python3.6/site-packages/flask/app.py", line 2292, in wsgi_app
        response = self.full_dispatch_request()
      File "/Users/glock/.apps/anaconda3/lib/python3.6/site-packages/flask/app.py", line 1815, in full_dispatch_request
        rv = self.handle_user_exception(e)
      File "/Users/glock/.apps/anaconda3/lib/python3.6/site-packages/flask/app.py", line 1718, in handle_user_exception
        reraise(exc_type, exc_value, tb)
      File "/Users/glock/.apps/anaconda3/lib/python3.6/site-packages/flask/_compat.py", line 35, in reraise
        raise value
      File "/Users/glock/.apps/anaconda3/lib/python3.6/site-packages/flask/app.py", line 1813, in full_dispatch_request
        rv = self.dispatch_request()
      File "/Users/glock/.apps/anaconda3/lib/python3.6/site-packages/flask/app.py", line 1799, in dispatch_request
        return self.view_functions[rule.endpoint](**req.view_args)
      File "./pytokio_rest.py", line 252, in hdf5_dataset_route
        return tokio_tool_hdf5(file_system, hdf5_resource, datetime_start, datetime_end)
      File "./pytokio_rest.py", line 147, in tokio_tool_hdf5
        return format_output(json.loads(result_df.to_json(orient=orient)))
    AttributeError: 'NoneType' object has no attribute 'to_json'

