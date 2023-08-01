'''
upload-pmt - by giovanni.organtini@roma1.infn.it (2022), patrizia.barria@roma1.infn.ir, claudio.quaranta@roma1.infn.it
----------------------------------------------------------

Automatically upload data stored after the measurement on the PMT bench to the DB
'''

from mtdConstructionDBTools import mtdcdb
import pandas as pd
import time
import re
import sys
import os
import logging
import glob
import subprocess
import signal
import getopt
import uploaderconfig

# Standard configuration (TBC)
inputdir = 'udoodaq01'
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
shrtOpts = 'hrdv:'
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
        dryrun_ = True

if debug:
    logger.setLevel(logging.DEBUG)

files = glob.glob(uploaderconfig.DIRIN + f'/{inputdir}/*.csv')
if debug:
    logger.debug(f'Looking for files in {uploaderconfig.DIRIN}/{inputdir}')

# Create output dirs
os.system(f"mkdir -p {uploaderconfig.DIRUPLOADED}/{inputdir}")
os.system(f"mkdir -p {uploaderconfig.DIRFAILED}/{inputdir}")
os.system(f"mkdir -p {uploaderconfig.DIRPROCESSED}/{inputdir}")

# open tunnel to dbloader for query
os.system('ssh -f -N -L 8113:dbloader-mtd.cern.ch:8113 mtdloadb@lxplus.cern.ch')

csvHeader="runName,b_rms,producer,i1,i0,i3,i2,tag,decay_time,id,pe,lyRef,geometry,b_3s_asym,b_2s_asym,time,type,dt_raw,ly"
csvHeader_split = csvHeader.split(",")
for csvfile in files:


    # Initiate the session 
    mtdcdb.initiateSession(user = 'mtdloadb', write=True)

    # Open file for failed uploads
    csvfile_succeded = open(uploaderconfig.DIRUPLOADED + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1] , "w")
    csvfile_failed   = open(uploaderconfig.DIRFAILED   + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1] , "w")
    n_failed = 0

    # get the content of the csv file
    logger.info(f'Processing file {csvfile}')
    firstline = open(csvfile, "r+").readline().rstrip("\r\n")

    if "runName" in firstline:
        data = pd.read_csv(csvfile)
    else:
        data = pd.read_csv(csvfile, header=None, names=csvHeader_split) # if header is missing
    data['lyAbs'  ]    = data['ly']    / data['pe']
    data['lyAbs'].fillna(data['ly'], inplace=True) # fill lyAbs with ly if the result of ly/pe is nan (because pe is missing)
    data['lyNorm' ]    = data['ly'] / data['lyRef' ]
    
    # first get the different runs
    runs = data[csvHead]
    runSet = set(runs)

    # Iterate over runs (one for each bar):
    for run in runSet:

        # create the XML
        root = mtdcdb.root()
        filtered_rows = data.loc[data[csvHead] == run]

        run_begin = None
        run_begin = filtered_rows.iloc[0]['time']
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
        
        # Loop over bars in a run (one bar)
        for index, row in filtered_rows.iterrows():
            xdataset = {}

            bc = str(row['id'])
            barcode = str(row['id'])
            if len(bc) < 14:
                barcode = f'32110001{bc}' # new format for production
                if 'FK' in barcode:
                    barcode = f'{bc.zfill(10)}'
        
            omsVarNames = {
                'b_rms'      : 'B_RMS',
                'b_3s_asym' : 'B_3S_ASYM',
                'b_2s_asym' : 'B_2S_ASYM',
                'lyAbs'     : 'LY_ABS',
                'lyNorm'  : 'LY_NORM',
                'decay_time'   : 'DECAY_TIME',
            }

            # prepare dictionary
            xdata = []
            for var in omsVarNames.keys():
                if pd.notna(row[var]):
                    xdata.append({'NAME': omsVarNames[var], 'VALUE': row[var]})
               
            # pack data
            xdataset[barcode] = xdata
            if 'STP_PREIRR' in run_dict['NAME']:     # set version=0 for STP (producer) measurement, otherwise version=1.0 (Rome)
                condition = mtdcdb.newCondition(root, 'LY MEASUREMENT', xdataset, run = run_dict,
                                                version=0, runBegin = run_begin)                  
            else:
                condition = mtdcdb.newCondition(root, 'LY MEASUREMENT', xdataset, run = run_dict,
                                                runBegin = run_begin)

        # TBC ============== to here =========================================

        logger.info(f'   File contains data for bar {barcode}')

        # transfer data to DB
        if debug:
            print(mtdcdb.mtdxml(condition))
        mtdcdb.transfer(condition, dryrun=dryrun_, user='mtdloadb')


        ######################################
        # check the upload status using rhapi.py
        ######################################

        logger.info('Operation summary:')
        logger.info(f'Checking {barcode}')
    
        if debug:
            logger.debug('No write required -> no checking')
        else:
            time.sleep(2)

            r = subprocess.run('python3 /home/cmsdaq/MTDDB/rhapi.py --url=http://localhost:8113  ' '"select r.NAME  from mtd_cmsr.c1420 c join mtd_cmsr.datasets d on d.ID = c.CONDITION_DATA_SET_ID join  mtd_cmsr.runs r on r.ID = d.RUN_ID where r.name = \''  + run_dict['NAME'] + '\'"', shell = True, stdout = subprocess.PIPE)

            status = 'Fail'
            if "ERROR" in str(r.stdout):
                logger.info('\nrhapi failed to check part upload on DB.')
                logger.info('rhapi.py output:\n'+str(r.stdout))
                status = 'Unknown - considered as Failed'

            elif bc in str(r.stdout):
                status = 'Success'
                csvfile_succeded.write(filtered_rows[csvHeader_split].to_csv(index=False, header=False))

            if status != 'Success':
                n_failed += 1
                csvfile_failed.write(filtered_rows[csvHeader_split].to_csv(index=False, header=False))

            logger.info(f'Upload status for {bc}: {status}')
    if not debug:
        logger.info(f'Failed uploads: {n_failed}')

    # move the processed file (remove retried uploads)
    if retry_upload and debug==False:
        os.remove(csvfile)
    elif debug==False:
        csvfiledone = uploaderconfig.DIRPROCESSED + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1]
        logger.info(f'Moving {csvfile} to {csvfiledone}')
        os.rename(csvfile, csvfiledone)

    # remove file of failed uploads if empty
    csvfile_failed.close()
    if os.path.getsize(csvfile_failed.name) == 0:
        logger.info(f'No failed uploads, removed file {csvfile_failed.name}')
        os.remove(csvfile_failed.name)    

# search pid and close all ssh tunnel
tunnel_pid = subprocess.check_output(['pgrep', '-f', 'ssh.*8113:dbloader-mtd.cern.ch:8113'])
tunnel_pid = tunnel_pid.decode("utf-8")
pid_list = tunnel_pid.split("\n")
for pid in pid_list:
    if pid != '':
        os.kill(int(pid), signal.SIGKILL)
