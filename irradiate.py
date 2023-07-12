'''
    irradiatet.py - by giovanni.organtini@roma1.infn.it 2023

    This script is used to declare irradiations. It is rather 'manual', since this is not
    a routine operation and it does not worth writing a complex script.
'''
from mtdConstructionDBTools import mtdcdb
import getopt
import sys

runs = ['BAR400017_POSTIRR_0',
        'BAR400019_POSTIRR_0',
        'BAR400021_POSTIRR_0',
        'BAR400023_POSTIRR_0',
        'BAR400025_POSTIRR_0',
        'BAR400027_POSTIRR_0',
        'BAR400030_POSTIRR_0',
        'BAR400031_POSTIRR_0'
]

barcodes = ['321100001400017',
            '321100001400019',
            '321100001400021',
            '321100001400023',
            '321100001400025',
            '321100001400027',
            '321100001400030',
            '321100001400031'
]

# despite this is a quick & dirty script, we use the standard approach also to provide
# a documented example

hlp = (f'DECLARE IRRADIATIONS.\n')
logger, logginglevel = mtdcdb.createLogger()
shrtOpts, longOpts, helpOpts = mtdcdb.stdOptions()

shrtOpts += 'r:'
longOpts += 'run='
helpOpts += 'provides the run name/tag to be associated to this irradiation'

try:
    opts, args = getopt.getopt(sys.argv[1:], shrtOpts, longOpts)
except Exception as excptn:
    print("Unexpected exception: " + str(excptn))
    mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, -1, hlp)

barcode = None
run = None

for o, a in opts:
    if o in ('-h', '--help'):
        mtdcdb.mtdhelp(shrtOpts, longOpts, helpOpts, 0, hlp)
    elif o in ('-x', '--barcode'):
        barcode = a
    elif o in ('-r', '--run'):
        run = a

if barcode != None:
    barcodes = []
    barcodes.append(barcode)

if run != None:
    runs = []
    runs.append(run)

if len(runs) != len(barcodes):
    logger.error('The number of runs is not equal to the number of barcodes')
    exit(-1)
    
mtdcdb.initiateSession(user = 'organtin', port = 50022, write = True)

for run, barcode in zip(runs, barcodes):
    run = { 'TYPE': 'IRRADIATION_AT_CALLIOPE',
            'NAME': run}
    conditionDataset = { barcode: [{"name": "RADIATION_TYPE", "value": "G"},
                                   {"name": "DOSE", "value": "50"},
                                   {"name": "DOSERATE", "value": "5.4"}]
    }
    run_begin = '2023-06-05 17:10:21'

    root = mtdcdb.root()
    condition = mtdcdb.newCondition(root, 'IRRADIATIONS', conditionDataset, run = run,
                                    runBegin = run_begin)

    print(mtdcdb.mtdxml(condition))
    xmlfile = open(f'{barcode}-irr.xml', 'w')
    xmlfile.write(mtdcdb.mtdxml(condition))
    xmlfile.close()
#    mtdcdb.writeToDB(filename = f'{barcode}-irr.xml', dryrun = False, user = 'organtin', wait = 3)
  
