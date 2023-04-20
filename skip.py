'''

skip.py
author: giovanni.organtini@roma1.infn.it 2023

This script is used to generate the XML code needed to skip measurements on parts in the
MTD database. For information about how to use it, just run it.

'''

import sys
import subprocess
import getpass
import re
import os
import getopt
import logging
from mtdConstructionDBTools import mtdcdb

logger, logginglevel = mtdcdb.createLogger()

shrtOpts, longOpts, helpOpts = mtdcdb.stdOptions()

hlp = ('Generates the XML file needed to skip the measurements of an MTD part.\n' 
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

if username == None:
    username = getpass.getuser()
    
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

out = []

if tunnelUser == username:
    # open the tunnel even if we do not need to write the XML file. We need the
    # tunnel to get info about the batch
    if batchIngot != '':
        mtdcdb.initiateSession(user = tunnelUser, write = True, debug = True)

if barcode != '':
    skip = mtdcdb.xml2skip(barcode, user = username)
    out.append(barcode)
    
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
    skip = mtdcdb.xml2skip(out, user = username)

logger.debug('Skipping the following parts:')
logger.debug(out)
answer = input('      Are you sure? [y/N] ')

if answer in ('y', 'Y', 'yes', 'YES', 'Yes'):
    print(mtdcdb.mtdxml(skip))

if tunnelUser == username:
    mtdcdb.initiateSession(user = tunnelUser, write = write)
