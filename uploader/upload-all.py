'''
upload-all - by claudio.quaranta@roma1.infn.it (2022)
----------------------------------------------------------

Upload data stored after the measurement of LYSO parts properties on any bench:
 - Galaxy Array         : settings/galaxy_array.py
 - Galaxy Single Xtal   : settings/galaxy_bar.py
 - Array Bench          : settings/array_bench.py
 - Single Array bench   : settings/single_array_bench.py
 - PMT Single Xtal      : settings/pmt_single_xtal.py

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
shrtOpts = 'hs:rd'
longOpts = ['help','settings=', 'retry', 'debug']
helpOpts = ['[mandatory] settings file for selected bench as in dir settings/',
            'shows this help',
            'retry failed uploads',
            'DEBUG mode:\n - dryrun enabled (files are not uploaded)\n - print output XML file\n'
            ]

hlp = (f'Register measurements of ARRAYS Optical properties on OMS DB.\n'
       f'XML files required for registration are produced from .csv files in files_to_upload/.\n'
       f'If upload fails, the not uploaded measurements are saved in files_to_retry/.\n'
       f'To retry upload, run:\n\n'
       f'\t\t python3 upload-.py -r')

try:
    opts, args = getopt.getopt(sys.argv[1:], shrtOpts, longOpts)
except Exception as excptn:
    print("Unexpected exception: " + str(excptn))
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -1, hlp)

# Setting option default values
bench_settings = ''
retry_upload = False
debug    = False
dryrun_  = False

# parse input
for o, a in opts:
    if o in ('-h', '--help'):
        mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, 0, hlp)
    elif o in ('-s','--settings'):
        bench_settings = a
    elif o in ('-r', '--retry'):
        retry_upload = True
    elif o in ('-d', '--debug'):
        debug   = True
        dryrun_ = True
if debug:
    logger.setLevel(logging.DEBUG)

if bench_settings != '':
    print('===> settings %s <===' % bench_settings)
    importSetting = 'import %s as benchConf' % bench_settings.replace('/','.').split('.py')[0]
    print(importSetting)
    exec(importSetting)
else:
    print('Option -s (setting file) is mandatory. This is the file with the configuration of the bench to load data from.\n',
          'settings file are in settings/ dir.')
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -1, hlp)

inputdir   = benchConf.inputdir
runNameCol = benchConf.runNameCol
csvHeader  = benchConf.csvHeader

if retry_upload:
    logger.info(f'Retry failed uploads: moving files from {uploaderconfig.DIRFAILED}/{inputdir}/ to {uploaderconfig.DIRIN}/{inputdir}/')
    os.system(f'mv {uploaderconfig.DIRFAILED}/{inputdir}/*.csv {uploaderconfig.DIRIN}/{inputdir}/')
    
# Input dir
files = glob.glob(uploaderconfig.DIRIN + f'/{inputdir}/*.csv')
logger.info(f'Looking for files in {uploaderconfig.DIRIN}/{inputdir}')

# Create output dirs
os.system(f"mkdir -p {uploaderconfig.DIRUPLOADED}/{inputdir}")
os.system(f"mkdir -p {uploaderconfig.DIRFAILED}/{inputdir}")
os.system(f"mkdir -p {uploaderconfig.DIRPROCESSED}/{inputdir}")

# open tunnel to dbloader for query
os.system('ssh -f -N -L 8113:dbloader-mtd.cern.ch:8113 mtdloadb@lxplus.cern.ch')

csvHeader_split = csvHeader.split(",")
for csvfile in files:

    # get the content of the csv file
    logger.info(f'Processing file {csvfile}')
    firstline = open(csvfile, "r+").readline().rstrip("\r\n")

    if firstline == csvHeader:
        data = pd.read_csv(csvfile)
    else:
        data = pd.read_csv(csvfile, header=None, names=csvHeader_split) #  if header is missing
    data = benchConf.analyze(data)

    
    # Initiate the session 
    mtdcdb.initiateSession(user='mtdloadb', write=True)

    # Open files for failed/succeded uploads
    if not debug:
        csvfile_succeded = open(uploaderconfig.DIRUPLOADED + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1] , "w")
        csvfile_failed   = open(uploaderconfig.DIRFAILED   + f'/{inputdir}/' + csvfile.split(os.path.sep)[-1] , "w")
        n_failed = 0

    # Iterate over runs in file (one run for each array)
    runs = data[runNameCol]
    runSet = set(runs)
    for run in runSet:

        # create an XML structure per run
        root = mtdcdb.root()
        filtered_rows = data.loc[data[runNameCol] == run]

        run_begin = None
        m = re.search('_[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}', run)
        print(m)
        run_begin = m.group(0)
        run_begin = re.sub('_', '', run_begin)
        run_begin = re.sub('-([0-9]{2})-([0-9]{2})-([0-9]{2})$', ' \\1:\\2:\\3', run_begin) # date: YYYY-MM-DD-HH-MM-SS --> YYYY-MM-DD HH:MM:SS
        logger.info(f'   File contains run {run} started on {run_begin}')

        # get run info from the csv file
        run_dict = { 'NAME': run,
                     'TYPE': 'ARRAY',
                     'NUMBER': -1,
                     'COMMENT': '',
                     'LOCATION': 'CMS Roma Lab/ARRAY'
                 }

        # Loop on bars inside array
        for index, row in filtered_rows.iterrows():
            xdataset = {}
            
            # Format barcode
            bc = str(row['id'])
            barcode = f'{bc}'
            if 'FK' in barcode:
                barcode = f'{bc.zfill(10)}'
            elif len(bc) < 14:
                barcode = 'PRE'+f'{bc.zfill(10)}'
            if benchConf.parttype == 'array':
                barcode   += '-'+str(row['bar']) # add number of bar in array, or nothing if array itself

            # prepare dictionary
            xdata = []
            omsVarNames = benchConf.omsVarNames
            for var in omsVarNames.keys():
                if pd.notna(row[var]):
                    xdata.append({'NAME': omsVarNames[var], 'VALUE': row[var]})

            # pack data
            xdataset[barcode] = xdata
            condition = mtdcdb.newCondition(root, 'LY XTALK', xdataset, run = run_dict,
                                            runBegin = run_begin)
            
        # transfer data to DB
        logger.info(f'File contains data for {len(filtered_rows)} bars of matrix {bc}')
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
        
        if retry_upload:
            os.remove(csvfile) # move the processed file (remove retried uploads)
        else:
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