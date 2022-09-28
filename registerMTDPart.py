#!/usr/bin/python3
'''

registerMTDPart.py
author: giovanni.organtini@roma1.infn.it 2022

This script is used to generate the XML code needed to register parts into the
MTD database. For information about how to use it, just run it.

'''
from lxml import etree
import random
import getpass
import sys
import os
import subprocess
import getopt
import re
import logging
from mtdConstructionDBTools import mtdcdb

import geocoder
g = geocoder.ip('me')

logger = logging.getLogger(os.path.basename(__file__))
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logginglevel = logging.INFO

shrtOpts = 'hb:x:s:p:t:l:f:o:n:wu:d:c:Di'
longOpts = ['help', 'batch=', 'barcode=', 'serial=', 'producer=', 'type=', 'lab=', 'file=', 'output=',
            'n=', 'write', 'user=', 'data=', 'comment=', 'debug', 'int2r']
helpOpts = ['shows this help',
            'specify the batch to which the part belongs',
            'specify the barcode of the part',
            'specify the serial number of the part',            
            'specify the producer',
            'specify the part type',
            'specify the laboratory in which the registration is done',
            'the filename of a CSV file formatted such as the barcodes are under the \n' +
            '         "Barcode" column while the type is given in a column named "Type".\n' +
            '         The producer is expected in column "Producer". An optional column\n' +
            '         "Serial Number" is expected to contain the corresponding information.\n' +
            '         It can be left blank.',
            'the filename of the XML output file',
            'the number of barcodes to generate',
            'upload the XML file automatically at the end of the processing (requires --file)',
            'the CERN username authorised to permanently write data to DB \n' +
            '         (default to current username)',
            'producer provided data to be associated to the part',
            'operator comments',
            'activate debugging mode',
            'use test database int2r'
            ]

hlp = ('Generates the XML file needed to register one or more MTD parts.\n' 
       'If you provide a CSV file name, all the parts included\n'
       'in it are processed. If you provide a single barcode, this script generates\n' 
       'the XML for just the given barcode. Providing option n, generates n\n' 
       'parts whose barcode starts with the given barcode and ends with barcode + n.\n\n'
       'In order to ship data to CERN, you need to setup a tunnel as follows:\n'
       'ssh -L 50022:dbloader-mtd.cern.ch:22 <your-cern-username>@lxplus.cern.ch\n\n'
       'EXAMPLES:\n'
       './registerMTDPart.py')

try:
    opts, args = getopt.getopt(sys.argv[1:], shrtOpts, longOpts)
except Exception as excptn:
    print("Unexpected exception: " + str(excptn))
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -1, hlp)

batchIngot = ''
barcode = ''
serialNumber = None
producer = ''
partType = ''
csvfile = None
laboratory = g.city
xmlfile = os.path.basename(sys.argv[0]).replace('.py', '.xml')
nbarcodes = 1
write = False
username = None
comment = ''
pdata = ''
multiplicity = 16
attrs = None
database = 'cmsr'

laboratories = ['Roma', 'Milano', 'UVA', 'Caltech', 'CERN', 'Nebraska']

errors = 0

for o, a in opts:
    if o in ('-h', '--help'):
        mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, 0, hlp)
    elif o in ('-b', '--batch'):
        batchIngot = a
    elif o in ('-x', '--barcode'):
        barcode = a
    elif o in ('-s', '--serial'):
        serialNumber = a
    elif o in ('-f', '--file'):
        csvfile = a
    elif o in ('-p', '--producer'):
        producer = a
    elif o in ('-t', '--type'):
        if a in ['LYSOMatrix #1', 'LYSOMatrix #2', 'LYSOMatrix #3',
                 'singleCrystal #1', 'singleCrystal #2', 'singleCrystal #3',
                 'ETROC']:
            partType = a
    elif o in ('-l', '--lab'):
        if a in laboratories:
            laboratory = a
    elif o in ('-o', '--output'):
        xmlfile = a
    elif o in ('-n', '--n'):
        nn = int(a)
        if float(a).is_integer():
            nbarcodes = nn
    elif o in ('-w', '--write'):
        write = True
    elif o in ('-u', '--user'):
        username = a
    elif o in ('-d', '--data'):
        pdata = a
    elif o in ('-c', '--comment'):
        comment = a
    elif o in ('-s', '--single'):
        multiplicity = 0
    elif o in ('-D', '--debug'):
        logginglevel = logging.DEBUG
    elif o in ('-i', '--int2r'):
        database = 'int2r'
    else:
        assert False, 'unhandled option'

logger.setLevel(logginglevel)
logger.debug(f'Debugging mode ON')
logger.debug(f'output on {xmlfile}')
logger.debug(f'Registering part of type: {partType}')
logger.debug(f'Apparently you are in {g.city}')
logger.debug(f'Setting lab to {laboratory}')
logger.debug(f'        Username: {username}')
logger.debug(f'           Batch: {batchIngot}')
logger.debug(f'         Barcode: {barcode}')
logger.debug(f'   Serial number: {serialNumber}')
logger.debug(f'        Producer: {producer}')
logger.debug(f'csvfile (if any): {csvfile}')
logger.debug(f' Number of parts: {nbarcodes}')
logger.debug(f'         Comment: {comment}')
if len(pdata) > 0:
    logger.debug(f'   producer data: {pdata}')
if multiplicity > 1:
    logger.debug(f'    Multiplicity: {multiplicity}')
if write:
    logger.debug(f'Will write on DB {database}')
else:
    logger.debug(f'Will NOT write to DB {database}')

# make basic checks
if batchIngot == '':
    logger.error('batch/ingot information is mandatory. Please provide it')
    errors += 1

if barcode == '' and csvfile == None:
    logger.error('barcode or file are mandatory. Please provide one of them')
    errors += 1

if barcode != '' and csvfile == None and (producer == '' or partType == ''):
    logger.error('providing a barcode, requires to provide a producer, too, as well as a type')
    errors += 1    

if barcode != '' and csvfile != None:
    logger.error('either provide a barcode or a CSV file name')
    errors += 1

if csvfile != None and producer != '':
    logger.error('you provided a CSV file: the producer is expected to be given in the file')    
    errors += 1

if producer == '' and csvfile == None:
    logger.error('producer information is mandatory. Please provide it')
    errors += 1

if laboratory not in laboratories:
    logger.error(f'laboratory {laboratory} not found in the list of valid locations')
    errors += 1
    
if errors != 0:
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -2, hlp)

# check if output file exists
if os.path.exists(xmlfile):
    logger.error(f'XML {xmlfile} file already exists.\n' +
                 '                             Please rename or remove it before proceeding')
    exit(-1)
    
fxml = open(xmlfile, "w")
fxmlcond = open('cond-' + xmlfile, "w")
conditions = {}
condXml = None

# the root XML document containing parts
myroot = mtdcdb.root()
parts = etree.SubElement(myroot, "PARTS")

import pandas as pd

mtdcdb.initiateSession(username, write = write)

processedbarcodes = []

runDict = { 'NAME': 'VISUAL_INSPECTION',
            'LOCATION': g.city,
            'USER': username
        }

# processing CSV file 
if csvfile != None:
    parts = pd.read_csv(csvfile, sep = None, engine = 'python')
    # normalise column headers ignoring case, leading and trailing spaces and unwanted characters
    parts.columns = parts.columns.str.lower()
    parts.columns = parts.columns.str.strip()
    parts.columns = parts.columns.str.replace(' ','')

    logger.info(parts)

    for index, row in parts.iterrows():    
        partType = row['type'].strip()
        barcode = 'PRE{:010d}'.format(int(row['barcode'])) # check
        producer = row['producer']
        comment = str(row['comments'])
        if 'pdata' in parts.columns: 
            pdata = str(row['pdata'])
        if condXml == None:
            condXml = mtdcdb.root()
        if comment == "nan":
            comment = ''
        if pdata == "nan":
            pdata = ''
        if len(pdata) > 0 or len(comment) > 0:
            conditions[barcode] = [{'NAME': 'BATCH_INGOT_DATA', 'VALUE': pdata},
                                   {'NAME': 'OPERATORCOMMENT', 'VALUE': comment}
            ]
        serialNumber = None
        if 'serialnumber' in parts.columns:
            serialNumber = str(row['serialnumber']).strip()
            if len(serialNumber) == 0 or serialNumber == 'nan':
                serialNumber = None
            if serialNumber != None and len(serialNumber) > 40:
                l = len(serialNumber)
                logger.error(f'Serial number {serialNumber} for part {barcode} too long\n' + 
                              f'       the maximum allowed length is 40; it is {l}...exiting...')
                mtdcdb.terminateSession(username)
                exit(-1)
        loggerString = 'Registering {barcode} of type {partType} made by producer {producer}'
        if serialNumber != None:
            logger.info(loggerString + f' (serial #: {serialNumber})')
        else:
            logger.info(loggerString)
        partXML = part(barcode, partType, batch = batchIngot, attributes = attrs, user = username,
                       location = laboratory, manufacturer = producer, serial = serialNumber)
        processedbarcodes.append(barcode)

    fxml.write(mtdcdb.mtdxml(myroot))
    if condXml != None:
        condXml = mtdcdb.newCondition(condXml, 'PARTREGISTRATION', conditions, run = runDict); # check
        fxmlcond.write(mtdcdb.mtdxml(condXml))

elif barcode != '':
    bc = barcode
    if len(barcode) != 13:
        try:
            bc = int(barcode)
        except ValueError:
            logger.error('barcode must be an integer or have a length of 13')
            mtdcdb.terminateSession(username)
            exit(-1)
    condXml = mtdcdb.root()
    # processing parts with given name
    for i in range(nbarcodes):
        sbarcode = str(bc)
        if len(sbarcode) != 13:
            sbarcode = 'PRE{:010d}'.format(bc)            
        logger.info(f'Registering {sbarcode} of type {partType} made by producer {producer}')
        partxml = mtdcdb.part(sbarcode, partType, batch = batchIngot, attributes = attrs, user = username,
                              location = laboratory, manufacturer = producer, serial = serialNumber)
        parts.append(partxml)
        processedbarcodes.append(sbarcode)
        if len(pdata) > 0 or len(comment) > 0:
            conditions[sbarcode] = [{'NAME': 'BATCH_INGOT_DATA', 'VALUE': pdata},
                                    {'NAME': 'OPERATORCOMMENT',  'VALUE': comment}]
        bcnums = list([c for c in bc if c.isnumeric()])
        bcnum = "".join(bcnums)
        bc = bc.replace(bcnum, str(int(bcnum) + 1))
    fxml.write(mtdcdb.mtdxml(myroot))
    condXml = mtdcdb.newCondition(condXml, 'PART_REGISTRATION', conditions, run = runDict) # check
    fxmlcond.write(mtdcdb.mtdxml(condXml))

fxml.close()
fxmlcond.close()

if write:
    logger.info(f'***** PLEASE RED *************************************')
    logger.info(f'      You are about to write data on the DB {database}')
    answer = input('      Are you sure? [y/N] ')
    loggerString = ''
    if answer in ('y', 'Y', 'yes', 'YES', 'Yes'):
        if type not in mtdcdb.allowedTypes() and database == 'cmsr':
            loggerString = f'Not allowed: you requested to register a part of type {type} '.join(
                f'to {database}, and this is not allowed. Skipping...')
        else:
            loggerString = f'Transferring XML to the dbloader (using {database})...'
            tb = False
            if database == 'int2r':
                tb = True
            mtdcdb.writeToDB(filename = xmlfile, user = username, testdb = tb)
            mtdcdb.writeToDB(filename = 'cond-' + xmlfile, user = username, testdb = tb)
    logger.info(loggerString + 'done')

mtdcdb.terminateSession(username)

# check the results using rhapi.py
logger.info('Operation summary:')
for barcode in processedbarcodes:
    logger.info(f'Checking {barcode}')
    if write:
        r = subprocess.run('python3 ./rhapi.py --url=http://localhost:8113  '
                           '"select * from mtd_' + database + '.parts p where p.barcode = \'' +
                           barcode + '\'" -s 10', shell = True, stdout = subprocess.PIPE)
        status = 'Fail'
        if barcode in str(r.stdout):
            status = 'Success'
        logger.info(f'{barcode}:  {status}')
    else:
        logger.debug('No write required -> no checking')

logger.debug('XML file content ========================================')
if logginglevel == logging.DEBUG:
    with open(xmlfile, 'r') as f:
        print(f.read())

exit(0)
