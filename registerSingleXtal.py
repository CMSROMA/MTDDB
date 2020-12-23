from lxml import etree
import random
import sys
from mtdConstructionDBTools import mtdcdb

def mtdhelp():
    print('Usage: registerSingleXtal.py [batch/ingot] [crystal barcode]')
    
if len(sys.argv) < 3:
    mtdhelp()
    exit(-1)

# create root element
root = mtdcdb.root()
parts = etree.SubElement(root, "PARTS")

# create the batch/ingot pair (the father)
attrs = []
father_barcode = str(sys.argv[1])
bi = mtdcdb.part(str(father_barcode), 'Batch/Ingot', attrs)

# create the child part
children = etree.SubElement(bi, "CHILDREN")
barcode = str(sys.argv[2])
xtal = mtdcdb.part(barcode, 'singleBarCrystal', attrs)

# append the child to the father
children.append(xtal)

# append the father to the root node
parts.append(bi)

# parint xml
xml = etree.tostring(root, encoding='UTF-8', standalone = 'yes', xml_declaration=True, pretty_print=True)
print(xml.decode("utf-8"))

exit(0)

