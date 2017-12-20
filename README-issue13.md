Right now the process is not working right when initializing vs. adding.  Here's the test cases:

    rm output.hdf5; ./bin/cache_collectdes.py --init-start 2017-12-13T00:00:00 --init-end 2017-12-14T00:00:00 --debug --num-bbnodes 288 --timestep 10 2017-12-13T00:00:00 2017-12-13T01:00:00

The above works.  However, when trying to then modify the HDF5,

    ./bin/cache_collectdes.py --debug 2017-12-13T00:15:00 2017-12-13T00:30:00

There are IndexErrors that pop up.  The difference in the second case is attaching versus init'ing.  The indexing stuff really needs to be factored out so it can be tested as a separate unit.
