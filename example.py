from mtdConstructionDBTools import mtdcdb

# The first step in creating an XML document is to create its root. This is achieved as follows
myroot = mtdcdb.root()

# There are few functions to generate XML tags for specific columns in tables.
# The most general tag is the PART tag. You need to create the part and append it to the root
aPart = mtdcdb.part('barcode', 'kind_of_part')
myroot.append(aPart)
print(mtdcdb.mtdxml(myroot))

# A part may have subparts.
# Once the part is created, you need to create the subpart and append it to the part.
# Then append the part to the root.
myroot = mtdcdb.root()
aPart = mtdcdb.part('barcode', 'kind_of_part')
aSubPart = mtdcdb.part('a_sub_part_barcode', 'kind_of_subpart', manufacturer = 'manufacturer_of_subpart')
aPart.append(aSubPart)
myroot.append(aPart)
print(mtdcdb.mtdxml(myroot))

# Parts may have attributes.
# Create a list of attributes. An attribute is a dictionary with two keys: NAME and VALUE.
# To each element of the dictionary assign the corresponding values, then build a list
# of dictionaries and use it to create the part. Finally you append subparts to part, then
# parts to root.
myroot = mtdcdb.root()
aPart = mtdcdb.part('barcode', 'kind_of_part')
attrs = []
for i in range(3):
    attr = dict()
    attr['NAME'] = 'parname_' + str(i + 1)
    attr['VALUE'] = 'parvalue_' + str(i + 1)
    attrs.append(attr)
aSubPart = mtdcdb.part('a_sub_part_barcode', 'kind_of_subpart', manufacturer = 'manufacturer_of_subpart',
                       attributes = attrs)
aPart.append(aSubPart)
myroot.append(aPart)
print(mtdcdb.mtdxml(myroot))
