'''
upload-tofpet - by giovanni.organtini@roma1.infn.it (2022)
----------------------------------------------------------

Automatically upload data stored after the measurement on the GALAXY bench to the DB
'''

from mtdConstructionDBTools import mtdcdb
import pandas as pd
import time
import re
import sys
import os
import subprocess
import signal
import logging
from datetime import datetime
import glob
import getopt
import uploaderconfig

# Standard configuration (TBC)
inputdir = 'galaxy-bars'
csvHead  = 'runName'
debug    = False
dryrun_  = False
retry_upload = False

# configure the logger
logger = logging.getLogger(os.path.basename(__file__))
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: %(name)s/%(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logginglevel = logging.INFO
logger.setLevel(logginglevel)

# Line command options
shrtOpts = 'hrd'
longOpts = ['help', 'retry', 'debug']
helpOpts = ['shows this help',
            'retry failed uploads',
            'DEBUG mode:\n - dryrun enabled (files are not uploaded)\n - print output XML file\n'
            ]

hlp = (f'Register measurements of ARRAYS Optical properties on OMS DB.\n'
       f'XML files required for registration are produced from .csv files in files_to_upload/{inputdir}.\n'
       f'If upload fails, the not uploaded measurements are saved in files_to_retry/{inputdir}.\n'
       f'To retry upload, run:\n\n'
       f'\t\t python3 upload-{inputdir}.py -r')

try:
    opts, args = getopt.getopt(sys.argv[1:], shrtOpts, longOpts)
except Exception as excptn:
    print("Unexpected exception: " + str(excptn))
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -1, hlp)

for o, a in opts:
    if o in ('-h', '--help'):
        mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, 0, hlp)
    elif o in ('-r', '--retry'):
        logger.info(f'Retry failed uploads: moving files from {uploaderconfig.DIRFAILED}/{inputdir}/ to {uploaderconfig.DIRIN}/{inputdir}/')
        retry_upload = True
        os.system(f'mv {uploaderconfig.DIRFAILED}/{inputdir}/*.csv {uploaderconfig.DIRIN}/{inputdir}/')
    elif o in ('-d', '--debug'):
        debug   = True
        dryryb_ = True

if debug:
    logger.setLevel(logging.DEBUG)

files = glob.glob(uploaderconfig.DIRIN + f'/{inputdir}/*.csv')
if debug:
    logger.debug(f'Looking for files in {uploaderconfig.DIRIN}/{inputdir}')

# Create output dirs
os.system(f"mkdir -p {uploaderconfig.DIRUPLOADED}/{inputdir}")
os.system(f"mkdir -p {uploaderconfig.DIRFAILED}/{inputdir}")
os.system(f"mkdir -p {uploaderconfig.DIRPROCESSED}/{inputdir}")

csvHeader = "runName,id,time,L_bar_mu,L_bar_std,L_maxVar_LS,L_maxVar_LN,L_std_LS,L_std_LN,L_std_tot,L_max,L_mean,L_mean_std,L_mean_mitu,L_std_mitu,W_maxVar_LO,W_maxVar_LE,W_std_LO,W_std_LE,W_std_tot,W_max,W_mean,W_mean_std,W_mean_mitu,W_std_mitu,T_maxVar_FS,T_std_FS,T_max,T_mean,T_mean_std,T_mean_mitu,T_std_mitu,bar,bar_length,bar_length_std,type"

# open tunnel to dbloader for query
os.system('ssh -f -N -L 8113:dbloader-mtd.cern.ch:8113 mtdloadb@lxplus.cern.ch')

for csvfile in files:
    # get the content of the csv file
    logger.info(f'Processing file {csvfile}')
    firstline = open(csvfile, "r+").readline().rstrip("\r\n")

    # Check if header is missing
    if "runName" in firstline:
        data = pd.read_csv(csvfile)
        print("Header Found")
    else:
        data = pd.read_csv(csvfile, header=None, names=csvHeader.split(",")) #  if header is missing
        print("Header Not Found")

    # THERE IS ONLY ONE RUN IN EACH FILE FOR THE GALAXY BENCH BY DESIGN
    runs = data['runName']
    runSet = set(runs)

    if len(runSet) != 1:
        logger.error(f'   There were {len(runSet)} runs on this file')
        exit(-1)

    run = list(runSet)[0]
    run_begin = None

    run_begin = data.iloc[0]['time']
    print(run_begin)

    # convert date from string to datetime
    run_begin = datetime.strptime(run_begin, '%Y-%m-%d-%H-%M') #in use time format
    run_begin = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(run_begin.timestamp()))

    # m = re.search('_[0-2]{4}-[0-2]{2}-[0-2]{2}-[0-2]{2}-[0-2]{2}-[0-2]{2}', run)

    # run_begin = m.group(0)
    # run_begin = re.sub('_', '', run_begin)
    # run_begin = re.sub('-([0-2]{2})-([0-2]{2})-([0-2]{2})$', ' \\1:\\2:\\3', run_begin)

    logger.info(f'   File contains run {run} started on {run_begin}')

    # create the XML
    root = mtdcdb.root()

    # TBC ============== from here =========================================

    # get run info from the csv file
    run_dict = { 'NAME': data.iloc[0]['runName'],
                 'TYPE': 'Galaxy3D',
                 'NUMBER': -1,
                 'COMMENT': '',
                 'LOCATION': 'Roma/Galaxy3D'
             }

    for index, row in data.iterrows():
        xdataset = {}
        bar = str(row['id'])

        # format barcode, if needed
        barcode = str(row['id'])
        if not 'FK' in barcode:
            barcode = 'PRE{:010d}'.format(int(row['id']))
        print(barcode)
        # read data from csv

        #++++++++++++++++++++++++++++++
        #      GALAXY  BARS
        #++++++++++++++++++++++++++++++

        L_mean = row['L_mean']
        L_mean_std = row['L_mean_std']
        W_mean = row['W_mean']
        W_mean_std = row['W_mean_std']
        T_mean = row['T_mean']
        T_mean_std = row['T_mean_std']

        # prepare the dictionary


        xdata = [{'NAME': 'L_MEAN', 'VALUE': L_mean},
                 {'NAME': 'L_MEAN_STD',    'VALUE': L_mean_std},
                 {'NAME': 'W_MEAN', 'VALUE': W_mean},
                 {'NAME': 'W_MEAN_STD',    'VALUE': W_mean_std},
                 {'NAME': 'T_MEAN', 'VALUE': T_mean},
                 {'NAME': 'T_MEAN_STD',    'VALUE': T_mean_std}]


        # pack data
        xdataset[barcode] = xdata
        condition = mtdcdb.newCondition(root, 'XTAL DIMENSIONS', xdataset, run = run_dict,
                                        runBegin = run_begin)

    # TBC ============== to here =========================================

    logger.info(f'   File contains data for bar {barcode}')

    # transfer data to DB
    try:
        mtdcdb.initiateSession(user='mtdloadb')
    except Exception as err:
        print(f'Cannot proceed: {err}')
        exit(-1)
    if debug:
        print(mtdcdb.mtdxml(condition))
    mtdcdb.transfer(condition, dryrun=dryrun_, user='mtdloadb')

    ######################################3    
    # check the upload status using rhapi.py
    logger.info('Operation summary:')
    logger.info(f'Checking {barcode}')
    
    if not debug:
        time.sleep(2)

        r = subprocess.run('python3 ../rhapi.py --url=http://localhost:8113  ' '"select r.NAME  from mtd_cmsr.c3400 c join mtd_cmsr.datasets d on d.ID = c.CONDITION_DATA_SET_ID join  mtd_cmsr.runs r on r.ID = d.RUN_ID where r.name = \''  + data['runName'] + '\'" ', shell = True, stdout = subprocess.PIPE)
        
        status = 'Fail'
        if barcode in str(r.stdout):
            status = 'Success'

            # copy the file to upload succeded 
            csvfileuploaded = uploaderconfig.DIRUPLOADED + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1]
            logger.info(f'   Copying {csvfile} to {csvfileuploaded}')
            os.system(f'cp {csvfile} {csvfileuploaded}')

        elif "ERROR" in str(r.stdout):
            logger.info('\nrhapi failed to check part upload on DB.')
            logger.info('rhapi.py output:\n'+str(r.stdout))
            status = 'unknown'

            # copy the file to upload failed
            csvfilefailed = uploaderconfig.DIRFAILED + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1]
            logger.info(f'   Copying {csvfile} to {csvfilefailed}')
            os.system(f'cp {csvfile} {csvfilefailed}')

        else:
            # copy the file to upload failed
            csvfilefailed = uploaderconfig.DIRFAILED + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1]
            logger.info(f'   Copying {csvfile} to {csvfilefailed}')
            os.system(f'cp {csvfile} {csvfilefailed}')

        # move file to processed directory
        csvfiledone = uploaderconfig.DIRPROCESSED + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1]
        logger.info(f'   Moving {csvfile} to {csvfiledone}')
        os.rename(csvfile, csvfiledone)

        logger.info(f'Upload status for {barcode}:  {status}')

    else:
        logger.debug('No write required -> no checking')

# search pid and close ssh tunnel
tunnel_pid = subprocess.check_output(['pgrep', '-f', 'ssh.*8113:dbloader-mtd.cern.ch:8113'])
os.kill(int(tunnel_pid), signal.SIGKILL)
    
logger.debug('XML file content ========================================')
if logginglevel == logging.DEBUG:
    with open(xmlfile, 'r') as f:
        print(f.read())




