'''
upload-pmt - by giovanni.organtini@roma1.infn.it (2022), patrizia.barria@roma1.infn.ir, claudio.quaranta@roma1.infn.it
----------------------------------------------------------

Automatically upload data stored after the measurement on the PMT bench to the DB
'''

from mtdConstructionDBTools import mtdcdb
import pandas as pd
import time
import re
import os
import logging
import glob

# configuration (TBC)
mydir = 'udoodaq01'
debug = False
dryrun_ = debug
csvHead = 'runName'

# configure the logger
logger = logging.getLogger(os.path.basename(__file__))
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: %(name)s/%(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logginglevel = logging.INFO
logger.setLevel(logginglevel)

if debug:
    logger.setLevel(logging.DEBUG)

# look for new files
import uploaderconfig
files = glob.glob(uploaderconfig.DIRIN + f'/{mydir}/*.csv')
if debug:
    logger.debug(f'Looking for files in {uploaderconfig.DIRIN}/{mydir}')

csvHeader="runName,b_rms,producer,i1,i0,i3,i2,tag,decay_time,id,pe,lyRef,geometry,b_3s_asym,b_2s_asym,time,type,ly"
for csvfile in files:

    # Initiate the session 
    mtdcdb.initiateSession(user = 'mtdloadb')

    # get the content of the csv file
    logger.info(f'Processing file {csvfile}')
    firstline = open(csvfile, "r+").readline().rstrip("\r\n")

    if "runName" in firstline:
        data = pd.read_csv(csvfile)
    else:
        data = pd.read_csv(csvfile, header=None, names=csvHeader.split(",")) # if header is missing

    # first get the different runs
    runs = data[csvHead]
    runSet = set(runs)

    # Iterate over runs (one for each bar):
    for run in runSet:

        # create the XML
        root = mtdcdb.root()
        filtered_rows = data.loc[data[csvHead] == run]

        run_begin = None
        print(run)
        print(filtered_rows)
        run_begin = filtered_rows.iloc[0]['time']
        print(run_begin)
        run_begin = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(run_begin))

        logger.info(f'   File contains run {run} started on {run_begin}')

        # TBC ============== from here =========================================

        # get run info from the csv file
        run_dict = { 'NAME': filtered_rows.iloc[0]['runName'],
                     'TYPE': 'PMT',
                     'NUMBER': -1,
                     'COMMENT': '',
                     'LOCATION': 'Roma/PMT'
        }
        print("Entry Run Tag "+ run_dict['NAME'])
        bc = ''
        # Loop over bars in a run (one bar)
        for index, row in filtered_rows.iterrows():
            xdataset = {}

            bc = str(row['id'])

            # format barcode
            barcode = str(row['id'])
            if not 'FK' in barcode:
                barcode = 'PRE{:010d}'.format(int(row['id']))
            print(barcode)

            # read data from csv

            lyAbs = row['ly']/row['pe']
            lyNorm = row['ly']/row['lyRef']
            b_rms = row['b_rms']
            b_3s_asym = row['b_3s_asym']
            b_2s_asym = row['b_2s_asym']
            decay_time = row['decay_time']

        
            # prepare the dictionary
            xdata = [{'NAME': 'B_RMS',      'VALUE': b_rms},
                     {'NAME': 'B_3S_ASYM',  'VALUE': b_3s_asym},
                     {'NAME': 'B_2S_ASYM',  'VALUE': b_2s_asym},
                     {'NAME': 'LY_ABS',     'VALUE': lyAbs},
                     {'NAME': 'DECAY_TIME', 'VALUE': decay_time},
                     {'NAME': 'LY_NORM',    'VALUE': lyNorm}]


               
            # pack data
            xdataset[barcode] = xdata
            condition = mtdcdb.newCondition(root, 'LY MEASUREMENT', xdataset, run = run_dict,
                                            runBegin = run_begin)

        # TBC ============== to here =========================================

        logger.info(f'   File contains data for bar of {bc}')

        # transfer data to DB
        if debug:
            print(mtdcdb.mtdxml(condition))
        mtdcdb.transfer(condition, dryrun=dryrun_, user='mtdloadb')

    # move the processed file
    csvfiledone = uploaderconfig.DIROUT + f'/{mydir}/' + csvfile.split(os.path.sep)[-1]
    logger.info(f'   Moving {csvfile} to {csvfiledone}')
    os.rename(csvfile, csvfiledone)


