'''
upload-array - by giovanni.organtini@roma1.infn.it (2022)
----------------------------------------------------------

Automatically upload data stored after the measurement on the ARRAY bench (4 arrays) to the DB
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
import glob
import getopt
import uploaderconfig

#cmd = '. /home/cmsdaq/miniconda3/etc/profile.d/conda.sh && conda activate my-rdkit-env' 
#subprocess.call(cmd, shell=True, executable='/bin/bash')

# activate conda
#os.system("conda activate mtddb")

# Standard configuration (TBC)
inputdir = 'pccmsdaq03'
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

csvHeader = "tag,producer,type,id,geometry,runName,temp,bar,posX,posY,ly,ctr,sigmaT,err_sigmaT,lyRef,ctrRef,sigmaTRef,xtLeft,xtRight" 
csvHeader_split = csvHeader.split(",")
for csvfile in files:

    # get the content of the csv file
    logger.info(f'Processing file {csvfile}')
    firstline = open(csvfile, "r+").readline().rstrip("\r\n")

    if "runName" in firstline:
        data = pd.read_csv(csvfile)
    else:
        data = pd.read_csv(csvfile, header=None, names=csvHeader_split) #  if header is missing
    data['ctr_norm']    = data['ctr'] / data['ctrRef']
    data['ly_norm' ]    = data['ly' ] / data['lyRef' ]
    data['sigmaT_norm'] = data['sigmaT'] / data['sigmaTRef']

    # first get the different runs
    runs = data[csvHead]
    runSet = set(runs)

    # Initiate the session 
    mtdcdb.initiateSession(user='mtdloadb', write=True)

    # Open file for failed uploads
    csvfile_succeded = open(uploaderconfig.DIRUPLOADED + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1] , "w")
    csvfile_failed   = open(uploaderconfig.DIRFAILED   + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1] , "w")
    n_failed = 0

    # Iterate over runs (one run for each array) :
    for run in runSet:

        # create an XML structure per run
        root = mtdcdb.root()
        filtered_rows = data.loc[data[csvHead] == run]

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
                     'LOCATION': 'CMS Roma Lab/ARRAY'
                 }

        bc = ''
        # Loop on bars inside array
        for index, row in filtered_rows.iterrows():
            xdataset = {}
            bc = str(row['id'])
            bar = row['bar']

            # format barcode, if needed
            barcode = f'{bc}-{bar}'
            if len(bc) < 15:
                barcode = f'{bc.zfill(10)}-{bar}'
                if 'FK' in barcode:
                    barcode = f'{bc.zfill(10)}-{bar}'

            omsVarNames = {
                'ctr'      : 'CTR', 
                'ctr_norm' : 'CTR_NORM', 
                'temp'     : 'TEMPERATURE', 
                'xtLeft'   : 'XTLEFT', 
                'xtRight'  : 'XTRIGHT', 
                'ly'       : 'LY', 
                'ly_norm'  : 'LY_NORM',
                'sigmaT'   : 'SIGMA_T', 
                'sigmaT_norm' : 'SIGMAT_NORM',
            }

            # prepare dictionary
            xdata = []
            for var in omsVarNames.keys():
                if pd.notna(row[var]):
                    xdata.append({'NAME': omsVarNames[var], 'VALUE': row[var]})

            # pack data
            xdataset[barcode] = xdata
            condition = mtdcdb.newCondition(root, 'LY XTALK', xdataset, run = run_dict,
                                            runBegin = run_begin)
            
        # TBC ============== to here =========================================

        logger.info(f'   File contains data for {len(filtered_rows)} bars of matrix {bc}')

        # transfer data to DB
        if debug:
            print(mtdcdb.mtdxml(condition))
        mtdcdb.transfer(condition, dryrun = dryrun_, user='mtdloadb')

        ######################################3    
        # check the upload status using rhapi.py
        logger.info('Operation summary:')
        logger.info(f'Checking {bc}')
    
        if debug:
            logger.debug('No write required -> no checking')
        else:
            time.sleep(2)

            r = subprocess.run('python3 ../rhapi.py --url=http://localhost:8113  ' '"select r.NAME  from mtd_cmsr.c1400 c join mtd_cmsr.datasets d on d.ID = c.CONDITION_DATA_SET_ID join  mtd_cmsr.runs r on r.ID = d.RUN_ID where r.name = \''  + run_dict['NAME'] + '\'" -s 17', shell = True, stdout = subprocess.PIPE)

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
    if retry_upload and debug == False:
        os.remove(csvfile)
    elif debug == False:
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