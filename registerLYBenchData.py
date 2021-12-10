from mtdConstructionDBTools import mtdcdb
import pandas as pd
import datetime
import re

filename = '~/Downloads/crystalsDB_LYBench_PMT.csv'
filename = '~/Downloads/lyDB_SiPM_Array.csv'
csv = pd.read_csv(filename)

# first get the different runs
runs = csv['tag']
runSet = set(runs)

# iterate over runs
for run in runSet:
    root = mtdcdb.root()
    filtered_rows = csv.loc[csv['tag'] == run]
    # extract the begin time of a run from its tag
    run_begin = None
    try:
        m = re.search('_[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}', run)
        run_begin = m[1:]
    except:
        run_begin = None
    # build the dictionary with xtalk
    xtalkdataset = {}
    for index, row in filtered_rows.iterrows():
        barcode = str(row['id']) + '-' + str(row['bar'])
        lyAbs = row['ly']
        lyNorm = lyAbs/row['lyRef']
        ctr = row['ctr']
        sigma_t = row['sigmaT']
        temperature = row['temp']
        xtLeft = row['xtLeft']
        xtRight = row['xtRight']
        xtalk = [{'NAME': 'CTR',         'VALUE': ctr},
                 {'NAME': 'SIGMA_T',     'VALUE': sigma_t},
                 {'NAME': 'TEMPERATURE', 'VALUE': temperature},
                 {'NAME': 'XTLEFT',      'VALUE': xtLeft},
                 {'NAME': 'XTRIGHT',     'VALUE': xtRight},
                 {'NAME': 'LY_NORM',     'VALUE': lyNorm}]
        xtalkdataset[barcode] = xtalk
    # create the condition
    condition = mtdcdb.newCondition(root, 'LY_XTALK', xtalkdataset, run = 'LYBench_PMT',
                                    comment = run, runBegin = run_begin)
    # dump the XML file
    print(mtdcdb.mtdxml(condition))


    

