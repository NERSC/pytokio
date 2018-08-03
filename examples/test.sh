SCRIPTS=(tokio.analysis.umami.py
         tokio.connectors.nersc_isdct-demo.py
         tokio.tools.hdf5-demo.py
         tokio.tools.hdf5-heatmap.py)

for i in ${SCRIPTS[@]}
do
    jupyter nbconvert --to script "$(sed -e 's/.py$/.ipynb/' <<< "$i")" --stdout \
    | sed -e "/get_ipython/d" \
    | python >/dev/null
done
