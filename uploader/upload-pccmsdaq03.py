'''
upload-array - by giovanni.organtini@roma1.infn.it (2022)
----------------------------------------------------------

Automatically upload data stored after the measurement on the ARRAY bench (4 arrays) to the DB
'''

from mtdConstructionDBTools import mtdcdb
import pandas as pd
import time
import re
import os
import logging
import glob
import subprocess

#cmd = '. /home/cmsdaq/miniconda3/etc/profile.d/conda.sh && conda activate my-rdkit-env' 
#subprocess.call(cmd, shell=True, executable='/bin/bash')

# activate conda
#os.system("conda activate mtddb")

# configuration (TBC)
mydir = 'pccmsdaq03'
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

csvHeader = "tag,producer,type,id,geometry,runName,temp,bar,posX,posY,ly,ctr,sigmaT,err_sigmaT,lyRef,ctrRef,sigmaTRef,xtLeft,xtRight" 
for csvfile in files:

    # get the content of the csv file
    logger.info(f'Processing file {csvfile}')
    firstline = open(csvfile, "r+").readline().rstrip("\r\n")

    if "runName" in firstline:
        data = pd.read_csv(csvfile)
    else:
        data = pd.read_csv(csvfile, header=None, names=csvHeader.split(",")) #  if header is missing

    # first get the different runs
    runs = data[csvHead]
    runSet = set(runs)

    # Initiate the session 
    mtdcdb.initiateSession(user='mtdloadb')


    # Iterate over runs (one run for each array) :
    for run in runSet:
        # create an XML structure per run
        root = mtdcdb.root()
        filtered_rows = data.loc[data[csvHead] == run]
        # print(run)
        # print(filtered_rows)
        # print("")

        run_begin = None
        m = re.search('_[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}', run)
        run_begin = m.group(0)
        run_begin = re.sub('_', '', run_begin)
        run_begin = re.sub('-([0-9]{2})-([0-9]{2})-([0-9]{2})$', ' \\1:\\2:\\3', run_begin)
        logger.info(f'   File contains run {run} started on {run_begin}')

        # TBC ============== from here =========================================

        # get run info from the csv file
        run_dict = { 'NAME': run,
                     'TYPE': 'ARRAY',
                     'NUMBER': -1,
                     'COMMENT': '',
                     'LOCATION': 'Roma/ARRAY'
                 }

        bc = ''
        # Loop on bars inside array
        for index, row in filtered_rows.iterrows():
            xdataset = {}
            bc = str(row['id'])
            bar = row['bar']

            # format barcode, if needed
            barcode = f'{bc}-{bar}'
            if len(bc) < 14:
                barcode = 'PRE'+f'{bc.zfill(10)}-{bar}'
                if 'FK' in barcode:
                    barcode = f'{bc.zfill(10)}-{bar}'
            
            # read data from csv
            lyAbs = row['ly']
            lyNorm = lyAbs/row['lyRef']
            ctr = row['ctr']
            ctr_norm = ctr/row['ctrRef']
            sigma_t = row['sigmaT']
            sigma_t_norm = sigma_t/row['sigmaTRef']
            temperature = row['temp']
            xtLeft = row['xtLeft']
            xtRight = row['xtRight']
        
            # prepare the dictionary
            xdata = [{'NAME': 'CTR',          'VALUE': ctr},
                     {'NAME': 'CTR_NORM',     'VALUE': ctr_norm},
                     {'NAME': 'TEMPERATURE',  'VALUE': temperature},
                     {'NAME': 'XTLEFT',       'VALUE': xtLeft},
                     {'NAME': 'XTRIGHT',      'VALUE': xtRight},
                     {'NAME': 'LY',           'VALUE': lyAbs},                     
                     {'NAME': 'LY_NORM',      'VALUE': lyNorm},
                     {'NAME': 'SIGMA_T',      'VALUE': sigma_t},
                     {'NAME': 'SIGMA_T_NORM', 'VALUE': sigma_t_norm}]
                
            # pack data
            xdataset[barcode] = xdata
            #print(xdataset)
            condition = mtdcdb.newCondition(root, 'LY XTALK', xdataset, run = run_dict,
                                            runBegin = run_begin)
            
        # TBC ============== to here =========================================

        logger.info(f'   File contains data for {len(data.index)} bars of matrix {bc}')

        # transfer data to DB
        if debug:
            print(mtdcdb.mtdxml(condition))
        mtdcdb.transfer(condition, dryrun = dryrun_, user='mtdloadb')

    # move the processed file
    csvfiledone = uploaderconfig.DIROUT + f'/{mydir}/' + csvfile.split(os.path.sep)[-1]
    logger.info(f'   Moving {csvfile} to {csvfiledone}')
    os.rename(csvfile, csvfiledone)

#os.system("conda deactivate")
