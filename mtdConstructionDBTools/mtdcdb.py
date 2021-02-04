from lxml import etree
import sys
import getpass

def attribute(parent, name, value):
    attribute = etree.SubElement(parent, "ATTRIBUTE")
    name = etree.SubElement(attribute, "NAME").text = name
    value = etree.SubElement(attribute, "VALUE").text = value
    return attribute
    
def part(barcode, kind_of_part, attributes, user = 'organtin', location = 'testLab'):
    part = etree.Element("PART", mode = "auto")
    kind_of_part = etree.SubElement(part, "KIND_OF_PART").text = kind_of_part
    barcode = etree.SubElement(part, "BARCODE").text = barcode
    record_insertion = etree.SubElement(part, "RECORD_INSERTION_USER").text = user
    location = etree.SubElement(part, "LOCATION").text = location
    predefined_attributes = etree.SubElement(part, 'PREDEFINED_ATTRIBUTES')
    for attr in attributes:
        attribute(predefined_attributes, attr['NAME'], attr['VALUE'])
    return part

def root():
    root = etree.Element("ROOT", encoding = 'xmlns:xsi=http://www.w3.org/2001/XMLSchema-instance')
    return root

def mtdcreateMatrix(barcode, Xtaltype, producer, batchIngot, laboratory):
    # create root element
    root = etree.Element("ROOT", encoding = 'xmlns:xsi=http://www.w3.org/2001/XMLSchema-instance')
    parts = etree.SubElement(root, "PARTS")

    # build the list of the attributes
    attrs = []
    attr = {}
    attr['NAME'] = 'PRODUCER'
    attr['VALUE'] = producer
    attrs.append(attr)

    # create the batch/ingot pair (the father)
    bi = part(str(batchIngot), 'Batch/Ingot', attrs, user = getpass.getuser(), location = laboratory)
    attrs = []

    # create the matrix part (a child of the batch)
    matrix = etree.SubElement(bi, "CHILDREN")
    LYSOMatrixtype = f'LYSOMatrix #{Xtaltype}'
    matrixxml = part(barcode, LYSOMatrixtype, attrs, user = getpass.getuser(), location = laboratory)

    # append the child to the father
    matrix.append(matrixxml)

    # create the single crystals as children of the matrix
    for i in range(16):
        singlextal = etree.SubElement(matrix, "CHILDREN")
        cbarcode = barcode + '-' + str(i)
        xtal = part(cbarcode, 'singleBarCrystal', [], user = getpass.getuser(), location = laboratory)
        singlextal.append(xtal)

    # append the father to the root node
    parts.append(bi)
        
    # parint xml
    xml = etree.tostring(root, encoding='UTF-8', standalone = 'yes', xml_declaration=True, pretty_print=True)
    print(xml.decode("utf-8"))

def mtdhelp(shrtOpts, longOpts, helpOpts, err = 0, hlp = ''):
    print('Usage: registerMatrix.py [options]')
    print('       ' + hlp)
    print()
    shrtOpts = shrtOpts.replace(':', '')
    longOpts = [s.replace('=', '=<value>') for s in longOpts]
    for i in range(len(shrtOpts)):
        print('       ' + shrtOpts[i] + ' ('+ longOpts[i] + '): ' + helpOpts[i])
    exit(err)

