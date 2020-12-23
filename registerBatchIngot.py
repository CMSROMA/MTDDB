from lxml import etree
import random
from mtdConstructionDBTools import mtdcdb
import sys

def mtdhelp():
    print('Usage: registerBatchIngot.py [barcode]')

if len(sys.argv) < 2:
    mtdhelp()
    exit(-1)

# create root element
root = mtdcdb.root()
parts = etree.SubElement(root, "PARTS")

# create the batch/ingot pair
attrs = []
barcode = str(sys.argv[1])
bi = mtdcdb.part(str(barcode), 'Batch/Ingot', attrs)

# append it to the root node
parts.append(bi)

# print xml
xml = etree.tostring(root, encoding='UTF-8', standalone = 'yes', xml_declaration=True, pretty_print=True)
print(xml.decode("utf-8"))

