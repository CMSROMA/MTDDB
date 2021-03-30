from lxml import etree
import sys
import os
import getpass
import subprocess
import re
import time
from datetime import datetime

def attribute(parent, name, value):
    attribute = etree.SubElement(parent, "ATTRIBUTE")
    name = etree.SubElement(attribute, "NAME").text = str(name)
    value = etree.SubElement(attribute, "VALUE").text = str(value)
    return attribute
    
def part(barcode, kind_of_part, attributes = None, manufacturer = None, user = 'organtin',
         location = 'testLab', serial = None):
    part = etree.Element("PART", mode = "auto")
    kind_of_part = etree.SubElement(part, "KIND_OF_PART").text = kind_of_part
    barcode = etree.SubElement(part, "BARCODE").text = barcode
    if serial != None:
        serial = etree.SubElement(part, "SERIAL_NUMBER").text = serial
    record_insertion = etree.SubElement(part, "RECORD_INSERTION_USER").text = user
    location = etree.SubElement(part, "LOCATION").text = location
    if manufacturer != None:
        manufacturer = etree.SubElement(part, "MANUFACTURER").text = 'Producer_' + str(manufacturer)
    predefined_attributes = etree.SubElement(part, 'PREDEFINED_ATTRIBUTES')
    if attributes != None:
        for attr in attributes:
            attribute(predefined_attributes, attr['NAME'], attr['VALUE'])
    return part

def root():
    root = etree.Element("ROOT", encoding = 'xmlns:xsi=http://www.w3.org/2001/XMLSchema-instance')
    return root

def mtdcreateMatrix(myroot, parts, barcode, Xtaltype, producer, batchIngot, laboratory, serial = 'None'):
    # build the list of the attributes, if any
    attrs = []
    attr = {}
    attrs.append(attr)
    # for the time being no attrs are foreseen
    attrs = None

    # create the batch/ingot pair (the father)
    bi = part(str(batchIngot), 'Batch/Ingot', attrs, user = getpass.getuser(), location = laboratory)
    attrs = []

    # create the matrix part (a child of the batch)
    matrix = etree.SubElement(bi, "CHILDREN")
    LYSOMatrixtype = f'LYSOMatrix #{Xtaltype}'
    matrixxml = part(barcode, LYSOMatrixtype, attrs, user = getpass.getuser(), location = laboratory,
                     manufacturer = producer, serial = serial)

    # append the child to the father
    matrix.append(matrixxml)

    # create the single crystals as children of the matrix
    singlextal = etree.SubElement(matrixxml, "CHILDREN")
    xtal = ''
    for i in range(16):
        cbarcode = barcode + '-' + str(i)
        xtal = part(cbarcode, 'singleBarCrystal', [], user = getpass.getuser(), location = laboratory)
        singlextal.append(xtal)

    # append the father to the root node
    parts.append(bi)
    return myroot

def mtdxml(root):
    # print xml
    return etree.tostring(root, encoding='UTF-8', standalone = 'yes', xml_declaration=True,
                          pretty_print=True).decode("utf-8")

def mtdhelp(shrtOpts, longOpts, helpOpts, err = 0, hlp = ''):
    print(f'Usage: {sys.argv[0]} [options]')
    if len(hlp) > 0:
        hlp = hlp.split('\n')
        for s in hlp:
            print('       ' + s)
    print()
    shrtOpts = shrtOpts.replace(':', '')
    longOpts = [s.replace('=', '=<value>') for s in longOpts]
    for i in range(len(shrtOpts)):
        print('       ' + shrtOpts[i] + ' ('+ longOpts[i] + '): ' + helpOpts[i])
    exit(err)

def newrun(name, comment = None, user = None, begin = None, location = None, runtype = None,
        runnumber = None):
    if begin == None:
        now = datetime.now()
        begin = now.strftime("%Y-%m-$d %H:%M:%S")
    return 0


def finishrun(run_id, user = None, time = None, comment = None, end = None): 
    if end == None:
        now = datetime.now()
        begin = now.strftime("%Y-%m-$d %H:%M:%S")
    return 0

def newcondition(part_id, run_id, kind_of_condition, user = None, comment = None):
    return 0

def visualInspectionComment(part_id, comment, user = None, time = None, location = 'Roma'):
    return 0
    
def writeToDB(port = 50022, filename = 'registerMatrixBatch.xml', dryrun = False):
    if filename != 'registerMatrixBatch.xml':
        user = getpass.getuser()
        if not dryrun:
            subprocess.run(['scp', '-P', str(port), '-oNoHostAuthenticationForLocalhost=yes',
                            filename, user + '@localhost:/home/dbspool/spool/mtd/int2r/' + filename])
        print('File uploaded...waiting for completion...')
        time.sleep(10)
        l = 'This is a dry run'
        if not dryrun:
            cp = subprocess.run(['ssh', '-p',
                                 str(port), user + '@localhost',
                                 'cat /home/dbspool/logs/mtd/int2r/' +
                                 filename],
                                capture_output = True)
            # get the last line of the log file
            l = str(cp.stdout)
        ret = -1
        lst = l.split('\\n')
        if len(lst) >= 2:
            l = lst[-2]
            if re.match('.* - commit transaction', l):
                ret = 0
                print('    SUCCESS    ')
        else:
            print('*** ERR *** ' + l)
    else:
        print('*** ERR *** xml filename is mandatory. Cannot use the default one')

