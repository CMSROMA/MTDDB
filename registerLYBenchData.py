from mtdConstructionDBTools import mtdcdb
import pandas as pd
import datetime
import re

debug = False

filename = '~/Downloads/crystalsDB_LYBench_PMT.csv'
filename = '~/Downloads/lyDB_SiPM_Array.csv'
filename = '~/Downloads/lyDB_SiPM_Tofpet.csv'
filename = './fake_Tofpet.csv'
csv = pd.read_csv(filename)

# first get the different runs
runs = csv['tag']
runSet = set(runs)

# ssh session management
# add the following line to your .ssh/config file
# ControlPath ~/.ssh/control-%h-%p-%r
mtdcdb.initiateSession()
# iterate over runs
for run in runSet:
    # create an XML structure per run
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
        parttype = row['type']
        barcode = row['id']
        # get the barcode: if it is in the form FK% it has been already formatted
        #                  otherwise, if it is a plain, short, integer, we need to
        #                  format it. For preproduction parts we prepend the string
        #                  PRE to the barcode
        if not 'FK' in barcode:
            barcode = 'PRE{:010d}'.format(int(row['id']))
        # if this is an array, the barcode is composed from the barcode of the array
        # followed by the bar index
        if parttype == 'array':
            barcode += '-' + str(row['bar'])
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
    #create the run    
    run_dict = { 'name': run,
                 'type': 'TOFPET',
                 'number': -1,
                 'comment': '',
                 'location': 'Roma/Tofpet'
        }
    # create the condition
    condition = mtdcdb.newCondition(root, 'LY XTALK', xtalkdataset, run = run_dict,
                                    runBegin = run_begin)
    # dump the XML file
    mtdcdb.transfer(condition)
    if debug:
        print(mtdcdb.mtdxml(condition))
    
mtdcdb.terminateSession()

