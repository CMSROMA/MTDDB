'''

registerMatrix.py
author: giovanni.organtini@roma1.infn.it 2020

This script is used to generate the XML code needed to register an MTD LYSO matrix into the
MTD database. For information about how to use it, just run it.

'''
from lxml import etree
import random
import getpass
import sys
import getopt
from mtdConstructionDBTools import mtdcdb

PRODUCER_MAX = 10

shrtOpts = 'hb:x:p:t:l:'
longOpts = ['help', 'batch=', 'xtal=', 'producer=', 'type=', 'lab=']
helpOpts = ['shows this help', 'specify the batch to which the matrix belongs',
            'specify the barcode of the matrix',
            'specify the producer index [0 < index < {}]'.format(PRODUCER_MAX),
            'specify the matrix type (thickness) [1, 2 or 3]',
            'specify the laboratory in which the registration is done (default = Roma)']

def mtdhelp(shrtOpts, longOpts, helpOpts, err = 0):
    print('Usage: registerMatrix.py [options]')
    print('       options')
    shrtOpts = shrtOpts.replace(':', '')
    longOpts = [s.replace('=', '=<value>') for s in longOpts]
    for i in range(len(shrtOpts)):
        print('       ' + shrtOpts[i] + ' ('+ longOpts[i] + '): ' + helpOpts[i])
    exit(err)
    
try:
    opts, args = getopt.getopt(sys.argv[1:], shrtOpts, longOpts)
except:
    mtdhelp(shrtOpts, longOpts, helpOpts, -1)

batchIngot = ''
barcode = ''
producer = ''
Xtaltype = ''
laboratory = 'Roma'

for o, a in opts:
    if o in ('h', '--help'):
        mtdhelp(shrtOpts, longOpts, helpOpts)
    elif o in ('b', '--batch'):
        batchIngot = a
    elif o in ('x', '--xtal'):
        barcode = a
    elif o in ('p', '--producer'):
        prd = int(a)
        if float(a).is_integer() and 0 < prd < (PRODUCER_MAX + 1):
            producer = a
    elif o in ('t', '--type'):
        tp = int(a)
        if float(a).is_integer() and 0 < tp < 4:
            Xtaltype = a
    elif o in ('l', '--lab'):
        if a in ['Roma', 'Milano', 'UVA', 'Caltech', 'CERN']:
            laboratory = a
    else:
        assert False, 'unhandled option'

# make basic checks
if batchIngot == '':
    print('[*** ERR ***] batch/ingot information is mandatory. Please provide it')

if barcode == '':
    print('[*** ERR ***] barcode is mandatory. Please provide it')

if producer == '':
    print('[*** ERR ***] producer information is mandatory. Please provide it')

if batchIngot == '' or barcode == '' or producer == '':
    mtdhelp(shrtOpts, longOpts, helpOpts, -2)

# create root element
root = mtdcdb.root()
parts = etree.SubElement(root, "PARTS")

# build the list of the attributes
attrs = []
attr = {}
attr['NAME'] = 'PRODUCER'
attr['VALUE'] = producer
attrs.append(attr)

# create the batch/ingot pair (the father)
bi = mtdcdb.part(str(batchIngot), 'Batch/Ingot', attrs, user = getpass.getuser(), location = laboratory)
attrs = []

# create the matrix part (a child of the batch)
matrix = etree.SubElement(bi, "CHILDREN")
LYSOMatrixtype = f'LYSOMatrix #{Xtaltype}'
matrixxml = mtdcdb.part(barcode, LYSOMatrixtype, attrs, user = getpass.getuser(), location = laboratory)

# append the child to the father
matrix.append(matrixxml)

# create the single crystals as children of the matrix
singlextal = etree.SubElement(matrixxml, "CHILDREN")
xtal = ''
for i in range(16):
    cbarcode = barcode + '-' + str(i)
    xtal = mtdcdb.part(cbarcode, 'singleBarCrystal', [], user = getpass.getuser(), location = laboratory)
    singlextal.append(xtal)

# append the father to the root node
parts.append(bi)

# parint xml
xml = etree.tostring(root, encoding='UTF-8', standalone = 'yes', xml_declaration=True, pretty_print=True)
print(xml.decode("utf-8"))

exit(0)

