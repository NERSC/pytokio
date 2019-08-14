import time
import json
import datetime
import tokio.common

OBJECT = {
    'thing1': datetime.datetime.now(),
    'thing2': { 'regular': 'old' },
    'thing3': {
        "list": [ "item1", "item2" ],
        "dict": {
            "another": datetime.datetime(2019, 1, 1),
        }
    },
    "set": { "item1", "item2", "item3" },
}

def test_JSONEncoder():
    """common.JSONEncoder"""
    json_blob = json.dumps(OBJECT, cls=tokio.common.JSONEncoder)
    print(json_blob)
    assert json_blob

def test_to_epoch():
    """common.to_epoch()"""
    now = datetime.datetime.now()
    if now.microsecond == 0:
        time.sleep(0.5)
        now = datetime.datetime.now()

    int_vers = tokio.common.to_epoch(now)
    flo_vers = tokio.common.to_epoch(now, float)

    assert int_vers
    assert flo_vers
    assert int_vers == int(flo_vers)
    assert flo_vers > int(int_vers) > 0.0
