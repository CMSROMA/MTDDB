from lxml import etree
import lxml.objectify
import sys
import os
import getpass
import subprocess
import re
import time
from datetime import datetime
import math

def attribute(parent, name, value):
    attribute = etree.SubElement(parent, "ATTRIBUTE")
    name = etree.SubElement(attribute, "NAME").text = str(name)
    value = etree.SubElement(attribute, "VALUE").text = str(value)
    return attribute
    
def part(barcode, kind_of_part, attributes = None, manufacturer = None, user = None,
         location = 'testLab', serial = None):
    part = etree.Element("PART", mode = "auto")
    kind_of_part = etree.SubElement(part, "KIND_OF_PART").text = kind_of_part
    barcode = etree.SubElement(part, "BARCODE").text = barcode
    if serial != None:
        serial = etree.SubElement(part, "SERIAL_NUMBER").text = serial
    if user == None:
        user = getpass.getuser()
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

# probably we can avoid it
def mtdcreateBatch(parts, batchIngot, user = None):
    # for the time being no attrs are foreseen
    attrs = None

    # create the batch/ingot pair (the father)
    if user == None:
        user = getpass.getuser()
    bi = part(str(batchIngot), 'Batch/Ingot', attributes = attrs, user = user)
    parts.append(bi)
#--------------------------

def mtdcreateMatrix(parts, barcode, Xtaltype, manufacturer, batchIngot, laboratory,
                    serial = 'None', user = None):
    # build the list of the attributes, if any
    attrs = []
    attr = {}
    attrs.append(attr)
    # for the time being no attrs are foreseen
    attrs = None

    # create the matrix part (a child of the batch)
    LYSOMatrixtype = f'LYSOMatrix #{Xtaltype}'
    matrixxml = part(barcode, LYSOMatrixtype, attributes = attrs, user = user, location = laboratory,
                     manufacturer = manufacturer, serial = serial)

    # create the single crystals as children of the matrix
    singlextal = etree.SubElement(matrixxml, "CHILDREN")
    xtal = ''
    for i in range(16):
        cbarcode = barcode + '-' + str(i)
        xtal = part(cbarcode, 'singleBarCrystal', attributes = [], user = user, location = laboratory)
        singlextal.append(xtal)

    # append the father to the root node
    parts.append(matrixxml)
    return parts

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

'''
def newcondition(part_id, run_id, kind_of_condition, user = None, comment = None):
    return 0

def visualInspectionComment(part_id, comment, user = None, time = None, location = 'Roma'):
    return 0
'''

def writeToDB(port = 50022, filename = 'registerMatrixBatch.xml', dryrun = False, user = None):
    if filename != 'registerMatrixBatch.xml':
        if user == None:
            user = getpass.getuser()
        if not dryrun:
            subprocess.run(['scp', '-P', str(port), '-oNoHostAuthenticationForLocalhost=yes',
                            filename, user + '@localhost:/home/dbspool/spool/mtd/int2r/' +
                            filename])
        for i in range(10, 0, -1):
            sys.stdout.write('File uploaded...waiting for completion...' + str(i)+'    \r')
            sys.stdout.flush()
            time.sleep(1)
        print('File uploaded...waiting for completion...done')
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
            print('(*) DB uploading can take time to complete. The above message is a guess.')
            print('    Please check using the web interface https://cmsdcadev.cern.ch/mtd_int2r/')
            print('    to verify if data has been, at least partially, uploaded.')
            print('    You can even browse the whole log file using the following command')
            print('    ssh -p 50022 <your-cern-username>@localhost cat /home/dbspool/logs/mtd/int2r/' +
                  filename)
    else:
        print('*** ERR *** xml filename is mandatory. Cannot use the default one')

def addDataSet(parent, barcode, dataset):
    part = etree.SubElement(parent, "PART")
    etree.SubElement(part, "BARCODE").text = barcode
    data = etree.SubElement(parent, "DATA")
    for d in dataset:
        name = d['name']
        if d['value']:
            etree.SubElement(data, name).text = d['value']
        
def condition(cmntroot, barcode, condition_name, condition_dataset, run = None, location = None,
              comment = None):
    if run == None:
        print('*** WARNING *** : conditions given, but no run details provided.')
        return
    cond = etree.SubElement(cmntroot, "HEADER")
    cond_type = etree.SubElement(cond, "TYPE")
    etree.SubElement(cond_type, "NAME").text = condition_name
    etree.SubElement(cond_type, "EXTENSION_TABLE_NAME").text = condition_name.replace(' ', '_')
    runElem = etree.SubElement(cond, "RUN")
    if type(run) is str:
        etree.SubElement(runElem, "RUN_NAME").text = run
    else:
        etree.SubElement(runElem, "RUN_TYPE").text = run['type']
        etree.SubElement(runElem, "RUN_NUMBER").text = run['number']
    if location != None:
        etree.SubElement(runElem, "LOCATION").text = location
    if comment != None:
        etree.SubElement(runElem, "COMMENT_DESCRIPTION").text = comment
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    etree.SubElement(runElem, "RUN_BEGIN_TIMESTAMP").text = current_time
    etree.SubElement(runElem, "RUN_END_TIMESTAMP").text = current_time
    dataset = etree.SubElement(cmntroot, "DATA_SET")
    addDataSet(dataset, barcode, condition_dataset)

def addVisualInspectionComment(cmntroot, barcode, comment = '', location = None, description = None,
                               pdata = None, batch = ''):
    aComment = condition(cmntroot, barcode,
                         'XTALREGISTRATION', [{"name": "OPERATORCOMMENT",
                                               "value": comment},
                                              {"name": "BATCH_INGOT",
                                               "value": batch},
                                              {"name": "BATCH_INGOT_DATA",
                                               "value": pdata}],
                         run = 'VISUAL_INSPECTION', location = location, comment = description)
