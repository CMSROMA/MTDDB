"""

This is btl_definition: a script to create the sql statements needed to configure the BTL construction DB.
It was written in 2020 by giovanni.organtini@roma1.infn.it

"""

def insert(table, values):
    # this function returns an insert sql statement
    columnList = ''
    valuesList = ''
    for column in values:
        columnList += str(column) + ', '
        valuesList += str(values[column]) + ', '
    columnList = columnList.rstrip(', ')
    valuesList = valuesList.rstrip(', ')
    sql = 'INSERT INTO ' + str(table) + ' (' + columnList + ') VALUES (' + valuesList + ');'
    return sql

class Component(object):
    # a class for the Composite UML design pattern
    # represents a generic detector component
    def __init__(self, name, description, id, subdetectorId = 2000, isVirtual = False, extension = 'PARTS'):
        self.name = name
        self.description = description
        self.children = []
        self.id = id
        self.subdetectorId = subdetectorId
        self.isVirtual = isVirtual
        self.extension = extension
    def set_id(self, id):
        self.id = id
    def get_id(self):
        return self.id
    def get_name(self):
        return self.name
    def get_description(self):
        return self.description
    def isParent(self):
        return False
    def get_components(self):
        return self.children
    def sqlInsert(self):
        columns = {}
        columns['KIND_OF_PART_ID'] = self.id
        columns['SUBDETECTOR_ID'] = self.subdetectorId
        columns['DISPLAY_NAME'] = "'{:s}'".format(self.name)
        columns['IS_DETECTOR_PART'] = "'T'"
        if self.isVirtual:
            columns['IS_DETECTOR_PART'] = "'F'"
        columns['COMMENT_DESCRIPTION'] = "'{:s}'".format(self.description)
        columns['EXTENSION_TABLE_NAME'] = "'" + self.extension + "'"
        columns['IS_IMAGINARY_PART'] = "'F'"
        return insert('CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS', columns)
    def sqlRelationship(self):
        return ''
    def sqlAttribute(self, id, name, n):
        kind_of_part_id = self.get_id()
        columns = {}
        attr_id = 100 * kind_of_part_id + id
        columns['ATTR_CATALOG_ID'] = attr_id
        columns['DISPLAY_NAME'] = "'" + name + "'"
        sqlAttr = insert('CMS_MTD_CORE_ATTRIBUTE.ATTR_CATALOGS', columns) + '\n'
        columns = {}
        columns['RELATIONSHIP_ID'] = attr_id
        columns['ATTR_CATALOG_ID'] = attr_id
        columns['KIND_OF_PART_ID'] = kind_of_part_id
        columns['DISPLAY_NAME'] = "'{:s} --> {:s}'".format(self.get_name(), name)
        sqlAttr += insert('CMS_MTD_CORE_CONSTRUCT.PART_TO_ATTR_RLTNSHPS', columns) + '\n'
        for i in range(n):
            columns = {}
            columns['ATTRIBUTE_ID'] = attr_id * 100 + i
            columns['ATTR_CATALOG_ID'] = attr_id
            columns['EXTENSION_TABLE_NAME'] = "'POSITION_SCHEMAS'"
            sqlAttr += insert('CMS_MTD_CORE_ATTRIBUTE.ATTR_BASES', columns) + '\n'            
            columns = {}
            columns['ATTRIBUTE_ID'] = attr_id * 100 + i
            columns['NAME'] = "'" + '0x{:02x}'.format(i) + "'"
            columns['IS_RECORD_DELETED'] = "'F'"
            sqlAttr += insert('CMS_MTD_CORE_ATTRIBUTE.POSITION_SCHEMAS', columns) + '\n'
        return sqlAttr
    
class Parent(Component):
    # a class for the Composite UML design pattern
    # represent an element of the subdetector composed by other elements
    def __init__(self, name, description, id, subdetectorId = 2000, isVirtual = False, extension = 'PARTS'):
        super().__init__(name, description, id)
    def setChildren(self, children):
        self.children = children
    def isParent(self):
        return True
    def sqlRelationship(self):
        ret = super().sqlRelationship()
        for c in self.get_components():
            columns = {}
            columns['RELATIONSHIP_ID'] = self.get_id() * 100000 + c.get_id()
            columns['KIND_OF_PART_ID'] = self.get_id()
            columns['KIND_OF_PART_ID_CHILD'] = c.get_id()
            columns['IS_RECORD_DELETED'] = "'F'"
            columns['DISPLAY_NAME'] = "'" + c.get_name() + ' --> ' + self.get_name() + "'"
            columns['PRIORITY_NUMBER'] = 0
            ret += insert('CMS_MTD_CORE_CONSTRUCT.PART_TO_PART_RLTNSHPS', columns)
            ret += c.sqlRelationship()
        return ret
    def sqlInsert(self):
        ret = super().sqlInsert()
        for c in self.get_components():
            ret += c.sqlInsert()
        return ret

class Child(Component):
    # a class for the Composite UML design pattern
    # represents a detector component not composed by sub-elements
    def __init__(self, name, description, id, subdetectorId = 2000, isVirtual = False, extension = 'PARTS'):
        super().__init__(name, description, id)

# define the parts of the detector, starting from the most elementary
batchIngot = Child('Batch/Ingot', 'Batch and ingot information for LYSO crystals', 1, isVirtual = True)

singleBarXtal = Child('singleBarCrystal', 'LYSO single crystal: used for quality control', 2, isVirtual = True)

sipmArray = Child('SiPM Array', 'Array of SiPM', 3)

# we attach a list of components to composed elements
lysoMatrix1 = Parent('LYSOMatrix #1', 'LYSO Matrix type #1', 4)
lysoMatrix1.setChildren([singleBarXtal])
lysoMatrix2 = Parent('LYSOMatrix #2', 'LYSO Matrix type #2', 5)
lysoMatrix2.setChildren([singleBarXtal])
lysoMatrix3 = Parent('LYSOMatrix #3', 'LYSO Matrix type #3', 6)
lysoMatrix3.setChildren([singleBarXtal])

btlModule1 = Parent('BTLModule #1', 'BTL Module made of a LYSO matrix type #1 and two SiPM arrays', 10)
btlModule1.setChildren([lysoMatrix1, sipmArray])
btlModule2 = Parent('BTLModule #2', 'BTL Module made of a LYSO matrix type #2 and two SiPM arrays', 11)
btlModule2.setChildren([lysoMatrix2, sipmArray])
btlModule3 = Parent('BTLModule #3', 'BTL Module made of a LYSO matrix type #3 and two SiPM arrays', 12)
btlModule3.setChildren([lysoMatrix3, sipmArray])

feCard = Child('FE', 'Front End Board', 7)

ccCard = Child('CC', 'Concentrator Card', 8)

roUnit1 = Parent('RU #1', 'Read out unit made of 24 BTL Modules, 4 FE and 1 CC', 100)
roUnit1.setChildren([btlModule1, feCard, ccCard])
roUnit2 = Parent('RU #2', 'Read out unit made of 24 BTL Modules, 4 FE and 1 CC', 101)
roUnit2.setChildren([btlModule2, feCard, ccCard])
roUnit3 = Parent('RU #3', 'Read out unit made of 24 BTL Modules, 4 FE and 1 CC', 102)
roUnit3.setChildren([btlModule3, feCard, ccCard])

tray = Parent('Tray', 'Tray containing six RU', 1000)
tray.setChildren([roUnit1, roUnit2, roUnit3])

btl = Parent('BTL', 'Barrel Timing Layer', 10000)
btl.setChildren([tray, batchIngot, singleBarXtal])

# this way we build a list containing a unique set of sql statements
insertSql = set(btl.sqlInsert().split(';'))
for i in insertSql:
    print(i + ';')

part2part = set(btl.sqlRelationship().split(';'))
for i in part2part:
    print(i + ';')

# create attributes

# singleCrystals
# ==============
# position in matrix (1, 16)
print(singleBarXtal.sqlAttribute(1, 'Position in matrix', 16))

# SiPM array
# ==========
# position in BTLModule (L, R)
print(sipmArray.sqlAttribute(1, 'Position in module', 2))

# BTL module
# ==========
# position in RU (1, 24) <--> ((1,3) <-> (1,8))
print(btlModule1.sqlAttribute(1, 'x-position in RU', 3))
print(btlModule1.sqlAttribute(2, 'y-position in RU', 8))
print(btlModule2.sqlAttribute(1, 'x-position in RU', 3))
print(btlModule2.sqlAttribute(2, 'y-position in RU', 8))
print(btlModule3.sqlAttribute(1, 'x-position in RU', 3))
print(btlModule3.sqlAttribute(2, 'y-position in RU', 8))

# FE
# ==
# position in RU (1, 4) <--> (TL, TR, LL, LR)
print(feCard.sqlAttribute(1, 'x-position in RU', 2))
print(feCard.sqlAttribute(2, 'y-position in RU', 2))

# RU
# ==
# position in Tray (1, 6)
print(roUnit1.sqlAttribute(1, 'position in Tray', 6))
print(roUnit2.sqlAttribute(1, 'position in Tray', 6))
print(roUnit3.sqlAttribute(1, 'position in Tray', 6))

# singleCrystals
# ==============
# x,y,z
# weight
# decay time
# ly




