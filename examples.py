import sys
import getopt
from mtdConstructionDBTools import mtdcdb

# get help with options
shrtopts = 'hwx:y:'
longopts = ['help', 'what', 'xml=', 'yetanotheroption=']
helpopts = ['shows this help',
            'what?',
            'specity an xml file',
            'a dummy one'
            ]

try:
    opts, args = getopt.getopt(sys.argv[1:], shrtopts, longopts)
except Exception as excptn:
    print("Unexpected exception: " + str(excptn))
    mtdcdb.mtdhelp(shrtopts, longopts, helpopts)

# upload a file to the dbloader
# mtdcdb.writeToDB(filename = 'gorgantini.xml', dryrun = True)

xml = mtdcdb.root()

attr = [{
        'NAME': 'attr1',
        'VALUE': 'val1'
        },{
        'NAME': 'attr2',
        'VALUE': 'val2'
        }
]
aPart = mtdcdb.part('01234567890123', 'type', batch = 'a batch', attributes = attr)
xml.append(aPart)

xmlstring = mtdcdb.mtdxml(xml)

print(xmlstring)

# ---------------------------------------------------------------------------

xml = mtdcdb.root()

xmlstring = mtdcdb.mtdxml(xml)

print(xmlstring)

dataset = {'01234567890': [{'NAME': 'name1', 'VALUE': 'value1'},
                           {'NAME': 'name2', 'VALUE': 'value2'},
                           {'NAME': 'name3', 'VALUE': 'value3'}],
           '09876543210': [{'NAME': 'name1', 'VALUE': 'valuea'},
                           {'NAME': 'name2', 'VALUE': 'valueb'},
                           {'NAME': 'name3', 'VALUE': 'valuec'}]
}
cond = mtdcdb.newCondition(xml, 'CONDITION_NAME', dataset, run = 'A_RUN_WITH_A_NAME')
xml.append(cond)

xmlstring = mtdcdb.mtdxml(xml)

print(xmlstring)
