from lxml import etree
import random
import sys

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
