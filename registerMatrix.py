'''

registerMatrix.py
author: giovanni.organtini@roma1.infn.it 2020

This script is used to generate the XML code needed to register an MTD LYSO matrix into the
MTD database. For information about how to use it, just run it.

'''
import sys
import getopt
from mtdConstructionDBTools import mtdcdb

PRODUCER_MAX = 10

# define command line options
shrtOpts = 'hb:x:p:t:l:'
longOpts = ['help', 'batch=', 'xtal=', 'producer=', 'type=', 'lab=']
helpOpts = ['shows this help', 'specify the batch to which the matrix belongs',
            'specify the barcode of the matrix',
            'specify the producer index [0 < index < {}]'.format(PRODUCER_MAX),
            'specify the matrix type (thickness) [1, 2 or 3]',
            'specify the laboratory in which the registration is done (default = Roma)']
hlp = 'Generates the XML needed to register a LYSO matrix into the MTD construction DB. Options follows.' \
    '\n       Example python3 {} --batch test-2020 --x 3310100000013 --producer 4 --t 1'.format(sys.argv[0])

try:
    opts, args = getopt.getopt(sys.argv[1:], shrtOpts, longOpts)
except:
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -1, hlp)

batchIngot = ''
barcode = ''
producer = ''
Xtaltype = ''
laboratory = 'Roma'

for o, a in opts:
    if o in ('h', '--help'):
        mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, hlp)
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

if batchIngot == '' or barcode == '' or producer == '':
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -2, hlp)

'''
def mtdcreateMatrix(barcode, producer, batchIngot, laboratory):
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
    for i in range(16):
        singlextal = etree.SubElement(matrix, "CHILDREN")
        cbarcode = barcode + '-' + str(i)
        xtal = mtdcdb.part(cbarcode, 'singleBarCrystal', [], user = getpass.getuser(), location = laboratory)
        singlextal.append(xtal)

    # append the father to the root node
    parts.append(bi)
        
    # parint xml
    xml = etree.tostring(root, encoding='UTF-8', standalone = 'yes', xml_declaration=True, pretty_print=True)
    print(xml.decode("utf-8"))
'''

mtdcdb.mtdcreateMatrix(barcode, Xtaltype, producer, batchIngot, laboratory)

exit(0)

