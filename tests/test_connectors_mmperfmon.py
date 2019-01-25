import json

import tokiotest
import tokio.connectors.mmperfmon

def test_get_col_pos():
    """connectors.mmperfmon.get_col_pos()
    """
    input_strs = [
        "Row           Timestamp cpu_user cpu_sys   mem_total",
        "Row   Timestamp cpu_user cpu_sys mem_total",
        "   Row   Timestamp     cpu_user cpu_sys mem_total",
        "Row   Timestamp     cpu_user cpu_sys mem_total     ",
        "  Row   Timestamp     cpu_user    cpu_sys      mem_total     ",
    ]
    for input_str in input_strs:
        print("Evaluating [%s]" % input_str)
        tokens = input_str.strip().split()
        offsets = tokio.connectors.mmperfmon.get_col_pos(input_str)
        print("Offsets are: " + str(offsets))
        assert offsets
        istart = 0
        num_tokens = 0
        for index, (istart, istop) in enumerate(offsets):
            token = input_str[istart:istop]
            print("    [%s] vs [%s]" % (token, tokens[index]))
            assert token == tokens[index]
            istart = istop
            num_tokens += 1
        assert num_tokens == len(tokens)

def test_to_df_by_host():
    """connectors.mmperfmon.Mmperfmon
    """
    mmpout = tokio.connectors.mmperfmon.Mmperfmon.from_file(tokiotest.SAMPLE_MMPERFMON_OUTPUT)
    print(json.dumps(mmpout, indent=4, sort_keys=True))
    assert mmpout

    for sample_host in tokiotest.SAMPLE_MMPERFMON_HOSTS:
        print("\nRetrieving dataframe for host [%s]" % sample_host)
        dataframe = mmpout.to_dataframe(by_host=sample_host)
        print(dataframe)
        assert(len(dataframe) > 0)

    for sample_metric in tokiotest.SAMPLE_MMPERFMON_METRICS:
        print("\nRetrieving dataframe for metric [%s]" % sample_metric)
        dataframe = mmpout.to_dataframe(by_metric=sample_metric)
        print(dataframe)
        assert(len(dataframe) > 0)
