from lxml import etree
import lxml.objectify
import sys
import os
import getpass
import subprocess
from subprocess import PIPE
import re
import time
from datetime import datetime
import math

'''
general services
'''
def opentunnel(user = None, port = 50022):
    if user == None:
        user = getpass.getuser()
    print('    need to open a tunnel...')
    subprocess.run(['ssh', '-f', '-N', '-L', str(port) + ':dbloader-mtd.cern.ch:22', 
                    '-L', '8113:dbloader-mtd.cern.ch:8113',
                    user + '@lxplus.cern.ch'])

def initiateSession(user = None, port = 50022, write = False):
    if user == None:
        user = getpass.getuser()
    if write:
        try:
            print('=== initiating session...')
            subprocess.check_call(['ssh', '-M', '-p', str(port), '-N', '-f', user + '@localhost'])
        except subprocess.CalledProcessError:
            opentunnel(user, port)
            print('=== retrying to initiate a session...')
            subprocess.run(['ssh', '-M', '-p', str(port), '-N', '-f', user + '@localhost'])

def terminateSession(user = None, port = 50022, write = False):
    if write:
        if user == None:
            user = getpass.getuser()
        print('=== terminating session...')
        subprocess.run(['ssh', '-O', 'exit', '-p', str(port), user + '@localhost'])
    
def mtdhelp(shrtOpts = '', longOpts = '', helpOpts = '', err = 0, hlp = ''):
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

def writeToDB(port = 50022, filename = 'registerMatrixBatch.xml', dryrun = False,
              user = None, wait = 10, testdb = False):
    dbname = 'cmsr'
    if filename != 'registerMatrixBatch.xml':
        if user == None:
            user = getpass.getuser()
        if not dryrun:
            xmlfile = os.path.basename(filename)
            if testdb:
                dbname = 'int2r'
            cmd = ['scp', '-P', str(port), '-oNoHostAuthenticationForLocalhost=yes',
                   filename, user + '@localhost:/home/dbspool/spool/mtd/' + dbname + '/' +
                   xmlfile]
            subprocess.run(cmd)
        print('File uploaded...waiting for completion...')
        l = 'This is a dry run'
        lst = ''
        if not dryrun:
            # wait until the log appears
            wait_until_log_appear = True
            while wait_until_log_appear:
                time.sleep(1)
                cp = subprocess.run(['ssh', '-q', '-p', str(port), 
                                     user + '@localhost', 'test', '-f', 
                                     '/home/dbspool/logs/mtd/' + dbname + '/' + xmlfile, 
                                     '&&', 'echo',  'Done!', '||', 'echo', '.', ';'], 
                                    stdout = PIPE, stderr = PIPE)
                testres = cp.stdout.decode("utf-8").strip()
                if len(testres) > 0:
                    print(testres[-1], end = '', flush = True)
                    if 'Done!' in testres:
                        wait_until_log_appear = False
                else:
                    print('Waiting for the log file to appear...')
                    time.sleep(5)
            # get the last line of the log file
            l = str(cp.stdout)
            ret = -1
            lst = l.split('\\n')
        if len(lst) >= 2 or dryrun:
            if not dryrun:
                l = lst[-2]
                if re.match('.* - commit transaction', l):
                    ret = 0
                    print('    SUCCESS    ')
                else:
                    ret = 0
                    print('    SUCCESS    ')
        else:
            print('*** ERR *** ' + l)
            print('(*) DB uploading can take time to complete. The above message is a guess.')
            print('    Please check using the web interface.')
            print('    to verify if data has been, at least partially, uploaded.')
            print('    You can even browse the whole log file using the following command')
            print('    ssh -p 50022 <your-cern-username>@localhost cat /home/dbspool/logs/mtd/' + dbname +
                  '/' + filename)
    else:
        print('*** ERR *** xml filename is mandatory. Cannot use the default one')

'''
create generic XML elements to register parts in the db
'''
def root():
    root = etree.Element("ROOT", encoding = 'xmlns:xsi=http://www.w3.org/2001/XMLSchema-instance')
    return root

def transfer(xml, filename = None, dryrun = False, user = None):
    path = filename
    if filename == None:
        path = '/tmp/' + str(time.time()) + '.xml'
    xmlfile = open(path, 'w')
    xmlfile.write(mtdxml(xml))
    xmlfile.close()
    writeToDB(filename = path, dryrun = dryrun, user = user, wait = 3)
    os.remove(path)
    
def mtdxml(root):
    # print xml
    return etree.tostring(root, encoding='UTF-8', standalone = 'yes', xml_declaration=True,
                          pretty_print=True).decode("utf-8")

def attribute(parent, name, value):
    attribute = etree.SubElement(parent, "ATTRIBUTE")
    name = etree.SubElement(attribute, "NAME").text = str(name)
    value = etree.SubElement(attribute, "VALUE").text = str(value)
    return attribute
    
def part(barcode, kind_of_part, batch = None, attributes = None, manufacturer = None, user = None,
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
    if batch != None and len(batch) > 0:
        batchIngot = etree.SubElement(part, "BATCH_NUMBER").text = str(batch)
    if manufacturer != None:
        manufacturer = etree.SubElement(part, "MANUFACTURER").text = 'Producer_' + str(manufacturer)
    predefined_attributes = etree.SubElement(part, 'PREDEFINED_ATTRIBUTES')
    if attributes != None:
        for attr in attributes:
            attribute(predefined_attributes, attr['NAME'], attr['VALUE'])
    return part

'''
helpers to create specific parts 
'''
def mtdcreateMatrix(parts, barcode, Xtaltype, manufacturer, batchIngot, laboratory,
                    serial = None, user = None, multiplicity = 16):
    # build the list of the attributes, if any
    attrs = []
    attr = {}
    attrs.append(attr)
    # for the time being no attrs are foreseen
    attrs = None

    # create the matrix part 
    LYSOMatrixtype = f'LYSOMatrix #{Xtaltype}'
    if multiplicity == 0:
        LYSOMatrixtype = f'singleCrystal #{Xtaltype}'
    matrixxml = part(barcode, LYSOMatrixtype, batch = batchIngot, attributes = attrs, user = user,
                     location = laboratory, manufacturer = manufacturer, serial = serial)

    # create the single crystals as children of the matrix
    if multiplicity > 0:
        singlextal = etree.SubElement(matrixxml, "CHILDREN")
    xtal = ''
    for i in range(multiplicity):
        cbarcode = barcode
        if multiplicity > 1:
            cbarcode += '-' + str(i)
        xtal = part(cbarcode, 'singleBarCrystal', attributes = [], user = user, location = laboratory)
        singlextal.append(xtal)

    # append the father to the root node
    parts.append(matrixxml)
    return parts

'''
helpers to create conditions
'''
def newCondition(cmntroot, condition_name, condition_dataset, run,
                 runBegin = None, runEnd = None):
    if cmntroot == None:
        cmntroot = root()
    if 'TYPE' in run.keys() and str(run['TYPE']) == None:
        print('*** WARNING *** : conditions given, but no run details provided.')
        return
    header = etree.SubElement(cmntroot, "HEADER")
    cond_type = etree.SubElement(header, "TYPE")
    etree.SubElement(cond_type, "NAME").text = condition_name
    extensionTableName = condition_name
    if 'REGISTRATION' in condition_name:
        extensionTableName = 'PART_REGISTRATION'
    etree.SubElement(cond_type, "EXTENSION_TABLE_NAME").text = extensionTableName.replace(' ', '_')
    runElem = newrun(header, run, begin = runBegin, end = runEnd)
    header.append(runElem)
    addDataSet(cmntroot, condition_dataset)
    return cmntroot

def newrun(condition, run = {}, begin = None, end = None):
    runElem = etree.SubElement(condition, "RUN")
    if begin == None:
        now = datetime.now()
        begin = now.strftime("%Y-%m-%d %H:%M:%S")
    if end == None:
        end = begin
    if str(run['NAME']) != '':
        etree.SubElement(runElem, "RUN_NAME").text = run['NAME']
    if 'TYPE' in run.keys():
        etree.SubElement(runElem, "RUN_TYPE").text = run['TYPE']
    if 'NUMBER' in run.keys() and int(run['NUMBER']) != -1:
        etree.SubElement(runElem, "RUN_NUMBER").text = run['NUMBER']
    etree.SubElement(runElem, "RUN_BEGIN_TIMESTAMP").text = begin
    etree.SubElement(runElem, "RUN_END_TIMESTAMP").text = end
    if 'COMMENT' in run.keys() and str(run['COMMENT']) != '':
        etree.SubElement(runElem, "COMMENT_DESCRIPTION").text = run['COMMENT']
    if str(run['LOCATION']) != '':
        etree.SubElement(runElem, "LOCATION").text = run['LOCATION']
    if not 'USER' in run or str(run['USER']) == '':
        run['USER'] = getpass.getuser()
    etree.SubElement(runElem, "INITIATED_BY_USER").text = run['USER']    
    return runElem

'''
def finishrun(run_id, user = None, time = None, comment = None, end = None): 
    if end == None:
        now = datetime.now()
        begin = now.strftime("%Y-%m-$d %H:%M:%S")
    return 0

def newcondition(part_id, run_id, kind_of_condition, user = None, comment = None):
    return 0

def visualInspectionComment(part_id, comment, user = None, time = None, location = 'Roma'):
    return 0
'''

def addDataSet(parent, dataset):
    for barcode in dataset:
        ds = etree.SubElement(parent, "DATA_SET")
        part = etree.SubElement(ds, "PART")
        etree.SubElement(part, "BARCODE").text = barcode
        data = etree.SubElement(ds, "DATA")
        actualData = dataset[barcode]
        for ad in actualData:
            for k,v in ad.items():
                ad.update({k.upper(): v})
            name = ad['NAME']
            if 'VALUE' in ad:
                etree.SubElement(data, name).text = str(ad['VALUE'])

'''                
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
'''

'''
helpers to create specific conditions
'''
def addVisualInspectionComment(cmntroot, barcode, comment = '', location = None, description = None,
                               pdata = None, batch = ''):
    conditionDataset = { barcode: [{"name": "OPERATORCOMMENT", "value": comment},
                                   {"name": "BATCH_INGOT_DATA","value": pdata}]
    }
    run = { 'type': 'VISUAL_INSPECTION' }
    aComment = newCondition(cmntroot, 'PARTREGISTRATION', conditionDataset,
                            run_type = 'VISUAL_INSPECTION', location = location,
                            comment = description)
