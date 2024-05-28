from mtdConstructionDBTools import mtdcdb
import pandas as pd
import math
import getopt
import datetime
import sys
import time
import re

debug   = False
dryrun  = False

shrtOpts = 'hdf:l:'
longOpts = ['help', 'debug', 'file=', 'location=']
helpOpts = ['shows this help',
            'activate debug mode (automatically set a dryrun)',
            'sets the csv filename to read from',
            'specify where the run was taken'
            ]

hlp = ('Reads a CSV file generated by a device for SiPM QCQA measurement and\n'
       'transfer data to the dbloader for storing them on the DB\n\n'
       'In order to ship data to CERN, you need to setup a tunnel as follows:\n'
       'ssh -L 50022:dbloader-mtd.cern.ch:22 <your-cern-username>@lxplus.cern.ch')

filename = None
location = 'Bicocca'

try:
    opts, args = getopt.getopt(sys.argv[1:], shrtOpts, longOpts)
except Exception as excptn:
    print("Unexpected exception: " + str(excptn))
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -1, hlp)

for o, a in opts:
    if o in ('-h', '--help'):
        mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, 0, hlp)
    elif o in ('-f', '--file'):
        filename = a
    elif o in ('-d', '--debug'):
        debug = True
        dryrun = True
    elif o in ('-l', '--location'):
        location = a

if filename == None:
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, 0, hlp)
    
csv = pd.read_excel(filename)

# ssh session management
# add the following line to your .ssh/config file
# ControlPath ~/.ssh/control-%h-%p-%r
if not debug:
    mtdcdb.initiateSession(user='organtin')

# extract the begin time of a run from its tag, if 
run_begin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
 
# build the dictionary with data
xdataset = {}
racs = {}
status = {}

barcode = None

for index, row in csv.iterrows():
    if not math.isnan(float(row['SN.'])):
        barcode = str(32110010000000 + int(row['SN.']))
    channel = row['Ch. No.']
    darki3v = row['Id[A] @VB+3V, 20 C']
    darki5v = row['Id[A] @VB-5V, 20 C']
    rfwd = row['Rforw [Ohm] @-1V, 20 C']
    vbrrt = row['VB[V], 20 C']
    if int(channel) == 1:
        if row['Array status'] != 'Pass':
            status[barcode] = row['Array status']
        rac = row['TEC[Ohm]']
        rtd = row['RTD[Ohm]']
        racs[barcode] = [{'NAME': 'RAC', 'VALUE': rac},
                         {'NAME': 'RTD', 'VALUE': rtd}]
        xdata = []
    xdata.append([{'NAME': 'CHANNEL',  'VALUE': channel},
                  {'NAME': 'DARKI3V',  'VALUE': darki3v},
                  {'NAME': 'DARKI5V',  'VALUE': darki5v},
                  {'NAME': 'RFWD',     'VALUE': rfwd},
                  {'NAME': 'VBRRT',    'VALUE': vbrrt}])
    xdataset[barcode] = xdata

#create the run    
run_dict = { 'NAME': filename,
             'TYPE': 'SiPMQCQA',
             'NUMBER': -1,
             'COMMENT': '',
             'LOCATION': location
}

# create the condition
root = mtdcdb.root()
condName = 'SIPM ARRAY COMMON DATA'
condition1 = mtdcdb.newCondition(root, condName, racs, run = run_dict,
                                runBegin = run_begin)
root = mtdcdb.root()
condName = 'SiPM QC QA'
condition2 = mtdcdb.newCondition(root, condName, xdataset, run = run_dict,
                                runBegin = run_begin)

# dump the XML files
mtdcdb.transfer(condition1, dryrun = dryrun, user='organtin')
if debug:
    print(mtdcdb.mtdxml(condition1))

mtdcdb.transfer(condition2, dryrun = dryrun, user='organtin')
if debug:
    print(mtdcdb.mtdxml(condition2))
    
if not debug:
    mtdcdb.terminateSession(user='organtin')

