'''

reject.py
author: giovanni.organtini@roma1.infn.it 2023

This script is used to generate the XML code needed to reject parts in the
MTD database. For information about how to use it, just run it.

'''

import sys
import re
import os
import subprocess
import getopt
import logging
from mtdConstructionDBTools import mtdcdb

# configure logger
logger = logging.getLogger(os.path.basename(__file__))
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logginglevel = logging.INFO

shrtOpts = 'hb:x:o:wu:c:DiT:'
longOpts = ['help', 'batch=', 'barcode=', 'output=', 'write', 'user=', 'comment=', 'debug',
            'int2r', 'tunnel=']
helpOpts = ['shows this help',
            'specify the batch to which the part belongs',
            'specify the barcode of the part',
            'the filename of the XML output file',
            'upload the XML file automatically at the end of the processing',
            'the CERN username authorised to permanently write data to DB \n' +
            '         (default to current username)',
            'operator comments',            
            'activate debugging mode',
            'use test database int2r',
            'tunnel user (default mtdloadb)'
            ]

hlp = ('Generates the XML file needed to set an MTD part as rejected.\n' 
       'In order to ship data to CERN, you may need to setup a tunnel as follows:\n'
       'ssh -L 50022:dbloader-mtd.cern.ch:22 <your-cern-username>@lxplus.cern.ch\n\n'
       'EXAMPLES:\n'
       './reject.py -x 33103000000834')

try:
    opts, args = getopt.getopt(sys.argv[1:], shrtOpts, longOpts)
except Exception as excptn:
    print("Unexpected exception: " + str(excptn))
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -1, hlp)

# set defaults
batchIngot = ''
barcode = ''
xmlfile = os.path.basename(sys.argv[0]).replace('.py', '.xml')
write = False
username = None
comment = ''
database = 'cmsr'
tunnelUser = None
debug = False
proxy = False

errors = 0

for o, a in opts:
    if o in ('-h', '--help'):
        mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, 0, hlp)
    elif o in ('-b', '--batch'):
        batchIngot = a
    elif o in ('-x', '--barcode'):
        barcode = a
    elif o in ('-o', '--output'):
        xmlfile = a
    elif o in ('-w', '--write'):
        write = True
    elif o in ('-u', '--user'):
        username = a
    elif o in ('-c', '--comment'):
        comment = a
    elif o in ('-D', '--debug'):
        logginglevel = logging.DEBUG
        debug = True
    elif o in ('-i', '--int2r'):
        database = 'int2r'
    elif o in ('-T', '--tunnel'):
        tunnelUser = a
        proxy = True
    else:
        assert False, 'unhandled option'

if tunnelUser == None:
    tunnelUser = username

# running conditions
logger.setLevel(logginglevel)
logger.debug(f'Debugging mode ON')
if barcode != '':
    logger.debug(f'Rejecting part {barcode}')
if batchIngot != '':
    logger.debug(f'Rejecting parts in batch {batchIngot}')    
logger.debug(f'output on {xmlfile}')
logger.debug(f'        Username: {username}')
logger.debug(f'     Tunnel user: {tunnelUser}')
logger.debug(f'         Comment: {comment}')
if write:
    logger.debug(f'Will write on DB {database}')
else:
    logger.debug(f'Will NOT write to DB {database}')

# make basic checks
if barcode != '' and batchIngot != '':
    logger.error('Cannot specify both barcode and batch: choose one')
    errors += 1

if barcode == '' and batchIngot == '':
    logger.error('Either barcode or batch must be specified')
    errors += 1

if errors != 0:
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -2, hlp)

if os.path.exists(xmlfile):
    logger.error(f'XML {xmlfile} file already exists.\n' +
                 '                             Please rename or remove it before proceeding')
    exit(-1)

# prepare command
command = 'python3 ./registerMTDPart.py'
if barcode != '':
    command += f' -x {barcode}'
if write:
    command += ' -w'
if comment != '':
    command += f' -c {comment}'
if debug:
    command += ' -D'
if database == 'int2r':
    command += ' -i'
if tunnelUser != None:
    command += f' -T {tunnelUser}'
if username != None:
    command += f' -u {username}'
    
command += f' -R'

# get list of parts, if needed
if batchIngot != '':
    query = f'python3 rhapi.py --url=http://localhost:8113 --all "select p.BARCODE from '
    query += f'mtd_{database}.parts p where p.BATCH_NUMBER = \'{batchIngot}\'"'
    logger.debug(f'{query}')
    p = subprocess.Popen(query, stdout=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    out = out.decode("utf-8").split('\n')
    # remove head (BARCODE) and tail ('')
    out.pop(0)
    out.pop()
    logger.debug('Rejecting the following parts:')
    logger.debug(out)
    answer = input('      Are you sure? [y/N] ')

    if answer in ('y', 'Y', 'yes', 'YES', 'Yes'):
        for b in out:
            xmlfileb = re.sub('.xml', f'-{b}.xml', xmlfile)
            if len(b) == 13:
                cmd = command + f' -x {b} -o {xmlfileb}'
                os.system(cmd)

elif barcode != '':
    command += f' -o {xmlfile}'
    logger.debug(command)
    os.system(command)

