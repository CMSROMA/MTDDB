#!/usr/bin/python3
'''

registerMatrixBatch.py
author: giovanni.organtini@roma1.infn.it 2020

This script is used to generate the XML code needed to register a batch of LYSO matrices into the
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
from mtdConstructionDBTools import mtdcdb

PRODUCER_MAX = 12

shrtOpts = 'hb:x:p:t:l:f:o:n:wu:d:c:s'
longOpts = ['help', 'batch=', 'barcode=', 'producer=', 'type=', 'lab=', 'file=', 'output=',
            'n=', 'write', 'user=', 'data=', 'comment=', 'single']
helpOpts = ['shows this help', 'specify the batch to which the matrix belongs',
            'specify the barcode of the matrix',
            'specify the producer index [0 < index < {}]'.format(PRODUCER_MAX),
            'specify the matrix type (thickness) [1, 2 or 3]',
            'specify the laboratory in which the registration is done (default = Roma)',
            'the filename of a CSV file formatted such as the barcodes are under the \n' +
            '         "Barcode" column while the tickness is given in mm in a column named "T (mm)".\n' +
            '         The producer is expected in column "Producer" (1-12). An optional column\n' +
            '         "Serial Number" is expected to contain the corresponding information.\n' +
            '         It can be left blank.',
            'the filename of the XML output file',
            'the number of barcodes to generate',
            'upload the XML file automatically at the end of the processing (requires --file)',
            'the CERN username authorised to permanently write data to DB (default to current username)',
            'producer provided data to be associated to the part',
            'operator comments',
            'register single crystals']

hlp = ('Generates the XML file needed to register one or more LYSO matrices or\n' 
       'single crystal. If you provide a CSV file name, all the parts included\n'
       'in it are processed. If you provide a single barcode, this script generates\n' 
       'the XML for just the given barcode. Providing option n, generates n.\n' 
       'matrices whose barcode starts with barcode and ends with barcode + n.\n\n'
       'In order to ship data to CERN, you need to setup a tunnel as follows:\n'
       'ssh -L 50022:dbloader-mtd.cern.ch:22 <your-cern-username>@lxplus.cern.ch\n\n'
       'EXAMPLES:\n'
       './registerMatrixBatch')

try:
    opts, args = getopt.getopt(sys.argv[1:], shrtOpts, longOpts)
except Exception as excptn:
    print("Unexpected exception: " + str(excptn))
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -1, hlp)

batchIngot = ''
barcode = ''
producer = ''
Xtaltype = ''
csvfile = None
laboratory = 'Roma'
xmlfile = os.path.basename(sys.argv[0]).replace('.py', '.xml')
nbarcodes = 1
write = False
username = None
comment = ''
pdata = ''
multiplicity = 16

errors = 0

for o, a in opts:
    if o in ('-h', '--help'):
        mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, 0, hlp)
    elif o in ('-b', '--batch'):
        batchIngot = a
    elif o in ('-x', '--barcode'):
        barcode = a
    elif o in ('-f', '--file'):
        csvfile = a
    elif o in ('-p', '--producer'):
        prd = int(a)
        if float(a).is_integer() and 0 < prd < (PRODUCER_MAX + 1):
            producer = a
        if not float(a).is_integer():
            errors += 1
            print('[*** ERR ***] the producer must be an integer')
        if prd < 1 or prd > PRODUCER_MAX:
            errors += 1
            print(f'[*** ERR ***] the producer must be within 1 and {PRODUCER_MAX}')
    elif o in ('-t', '--type'):
        tp = int(a)
        if float(a).is_integer() and 0 < tp < 4:
            Xtaltype = a
        if (not float(a).is_integer()) or tp < 1 or tp > 3:
            errors += 1
            print('[*** ERR ***] the type must be either 1, 2 or 3')
    elif o in ('-l', '--lab'):
        if a in ['Roma', 'Milano', 'UVA', 'Caltech', 'CERN']:
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
    else:
        assert False, 'unhandled option'

# make basic checks
if batchIngot == '':
    print('[*** ERR ***] batch/ingot information is mandatory. Please provide it')
    errors += 1

if barcode == '' and csvfile == None:
    print('[*** ERR ***] barcode or file are mandatory. Please provide one of them')
    errors += 1

if barcode != '' and csvfile == None and (producer == '' or Xtaltype == ''):
    print('[*** ERR ***] providing a barcode, requires to provide a producer, too, as well as a type')
    errors += 1    

if barcode != '' and csvfile != None:
    print('[*** ERR ***] either provide a barcode or a CSV file name')
    errors += 1

if csvfile != None and producer != '':
    print('[*** ERR ***] you provided a CSV file: the producer is expected to be given in the file')    
    errors += 1

if producer == '' and csvfile == None:
    print('[*** ERR ***] producer information is mandatory. Please provide it')
    errors += 1

if errors != 0:
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -2, hlp)

xtaltype = {
    "3,75": "1",
    "3,00": "2",
    "2,40": "3",
    "3.75": "1",
    "3.00": "2",
    "2.40": "3"
    }

# check if output file exists
if os.path.exists(xmlfile):
    print(f'XML {xmlfile} file already exists. Please rename or remove it before proceeding')
    exit(-1)
    
fxml = open(xmlfile, "w")
fxmlcond = open('cond-' + xmlfile, "w")
conditions = {}
condXml = None

# the root XML document containing parts
myroot = mtdcdb.root()
parts = etree.SubElement(myroot, "PARTS")

import pandas as pd

mtdcdb.initiateSession(username)

processedbarcodes = []

runDict = { 'NAME': 'VISUAL_INSPECTION',
            'LOCATION': 'Roma',
            'USER': username
        }

# processing CSV file 
if csvfile != None:
    matrices = pd.read_csv(csvfile, sep = None, engine = 'python')
    # normalise column headers ignoring case, leading and trailing spaces and unwanted characters
    matrices.columns = matrices.columns.str.lower()
    matrices.columns = matrices.columns.str.strip()
    matrices.columns = matrices.columns.str.replace(' ','')

    print(matrices)

    for index, row in matrices.iterrows():    
        Xtaltype = xtaltype[row['t(mm)']].strip()
        partType = f'LYSOMatrix #{Xtaltype}'
        barcode = 'PRE{:010d}'.format(int(row['barcode']))
        producer = row['producer']
        comment = str(row['comments'])
        if 'pdata' in matrices.columns: 
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
        if 'serialnumber' in matrices.columns:
            serialNumber = str(row['serialnumber']).strip()
            if len(serialNumber) == 0 or serialNumber == 'nan':
                serialNumber = None
            if serialNumber != None and len(serialNumber) > 40:
                l = len(serialNumber)
                print(f'Serial number {serialNumber} for part {barcode} too long')
                print(f'       the maximum allowed length is 40; it is {l}...exiting...')
                mtdcdb.terminateSession(username)
                exit(-1)
        ptype = 'matrix'
        if multiplicity == 0:
            ptype = 'bar'
        print(f'Registering {ptype} {barcode} of type {partType} made by producer {producer}', end = '')
        if serialNumber != None:
            print(f' (serial #: {serialNumber})')
        else:
            print()
        matrixxml = mtdcdb.mtdcreateMatrix(parts, barcode, Xtaltype, producer, batchIngot, laboratory,
                                           serial = serialNumber, user = username, multiplicity = multiplicity)
        processedbarcodes.append(barcode)

    fxml.write(mtdcdb.mtdxml(myroot))
    if condXml != None:
        condXml = mtdcdb.newCondition(condXml, 'XTALREGISTRATION', conditions, run = runDict);
        fxmlcond.write(mtdcdb.mtdxml(condXml))

elif barcode != '':
    bc = barcode
    if len(barcode) != 13:
        try:
            bc = int(barcode)
        except ValueError:
            print('[*** ERR ***] barcode must be an integer or have a length of 13')
            mtdcdb.terminateSession(username)
            exit(-1)
    condXml = mtdcdb.root()
    # processing parts with given name
    for i in range(nbarcodes):
        partType = f'LYSOMatrix #{Xtaltype}'
        if multiplicity == 0:
            Xtaltype = 'singleBarCrystal'
            partType = 'singleBarCrystal'
        sbarcode = str(bc)
        if len(sbarcode) != 13:
            sbarcode = 'PRE{:010d}'.format(bc)            
        print(f'Registering matrix {sbarcode} of type {Xtaltype} made by producer {producer}')
        myroot = mtdcdb.mtdcreateMatrix(parts, sbarcode, Xtaltype, producer, batchIngot, 
                                        laboratory, multiplicity = multiplicity, user = username)
        processedbarcodes.append(sbarcode)
        if len(pdata) > 0 or len(comment) > 0:
            conditions[sbarcode] = [{'NAME': 'BATCH_INGOT_DATA', 'VALUE': pdata},
                                    {'NAME': 'OPERATORCOMMENT',  'VALUE': comment}]
        bc += 1
    fxml.write(mtdcdb.mtdxml(myroot))
    condXml = mtdcdb.newCondition(condXml, 'XTALREGISTRATION', conditions, run = runDict)
    fxmlcond.write(mtdcdb.mtdxml(condXml))

fxml.close()
fxmlcond.close()

if write:
    print('Transferring XML to the dbloader...', end = '')
    mtdcdb.writeToDB(filename = xmlfile, user = username)
    mtdcdb.writeToDB(filename = 'cond-' + xmlfile, user = username)
    print('done')

mtdcdb.terminateSession(username)

# check the results using rhapi.py
print('Operation summary:')
for barcode in processedbarcodes:
    r = subprocess.run('/usr/bin/python3 ./rhapi.py --url=http://localhost:8113  '
                       '"select * from mtd_int2r.parts p where p.barcode = \'' +
                       barcode + '\'" -s 10', shell = True, stdout = subprocess.PIPE)
    print(barcode, end = ': ')
    if barcode in str(r.stdout) or not write:
        print('Success')
    else:
        print('Failed')

exit(0)

