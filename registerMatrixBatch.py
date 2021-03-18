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
import getopt
from mtdConstructionDBTools import mtdcdb

PRODUCER_MAX = 12

shrtOpts = 'hb:x:p:t:l:f:o:n:w'
longOpts = ['help', 'batch=', 'barcode=', 'producer=', 'type=', 'lab=', 'file=', 'output=',
            'n=', 'write']
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
            'upload the XML file automatically at the end of the processing (requires --file)']

hlp = ('Generates the XML file needed to register one or more LYSO matrices.\n' 
      'If you provide a CSV file name, all the matrices included in the file\n' 
      'are processed. If you provide a single barcode, this script generates\n' 
      'the XML for just the given barcode. Providing option n, generates n.\n' 
      'matrices whose barcode starts with barcode and ends with barcode + n')

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
xmlfile = sys.argv[0].replace('.py', '.xml')
nbarcodes = 1
write = False

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
    print('XML file already exists. Please rename or remove it before proceeding')
    exit(-1)
    
fxml = open(xmlfile, "w")

# the root XML document containing parts
myroot = mtdcdb.root()
parts = etree.SubElement(myroot, "PARTS")

import pandas as pd
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
        serialNumber = None
        if 'serialnumber' in matrices.columns:
            serialNumber = str(row['serialnumber']).strip()
            if len(serialNumber) == 0 or serialNumber == 'nan':
                serialNumber = None
        print(f'Registering matrix {barcode} of type {partType} made by producer {producer}', end = '')
        if serialNumber != None:
            print(f' (serial #: {serialNumber})')
        else:
            print()
        myroot = mtdcdb.mtdcreateMatrix(myroot, parts, barcode, Xtaltype, producer, batchIngot, laboratory,
                                        serial = serialNumber)
        
    fxml.write(mtdcdb.mtdxml(myroot))

elif barcode != '':
    try:
        bc = int(barcode)
    except ValueError:
        print('[*** ERR ***] barcode must be an integer')
        exit(-1)
    for i in range(nbarcodes):
        partType = f'LYSOMatrix #{Xtaltype}'
        sbarcode = 'PRE{:010d}'.format(bc)
        print(f'Registering matrix {sbarcode} of type {Xtaltype} made by producer {producer}')
        myroot = mtdcdb.mtdcreateMatrix(myroot, parts, sbarcode, Xtaltype, producer, batchIngot, laboratory)
        bc += 1
    fxml.write(mtdcdb.mtdxml(myroot))

fxml.close()

if write:
    writeToDB(filename = xmlfile)

exit(0)

