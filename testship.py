from mtdConstructionDBTools import mtdcdb

barcodes = range(842,852,1)
barcodes = [f'PRE{bc:010d}' for bc in barcodes] 
xml = mtdcdb.xml2ship(barcodes, from_institution = 'Roma', from_location = 'Roma',
                      to_institution = 'Bicocca', to_location = 'Bicocca')
print(mtdcdb.mtdxml(xml))
