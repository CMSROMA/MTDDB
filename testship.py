from mtdConstructionDBTools import mtdcdb

#barcodes = range(842,852,1)
#barcodes = [f'PRE{bc:010d}' for bc in barcodes] 

# Insert last 6 digits of the barcod of every shipped part
barcodes = [100023,100024,100025,100026,200040,200041,200042,200054,300013,300014]
barcodes = [f'3211{str(bc).zfill(10)}' for bc in barcodes] # auto-format barcode
tracking_number = '' # PLEASE insert shipping tracking number!!!!!

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# BAC Location: Bicocca, Virginia, Caltech, PKU (Peking University
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

xml = mtdcdb.xml2ship(barcodes, from_institution = 'Roma', from_location = 'Roma',
                      to_institution = 'Caltech', to_location = 'Caltech', 'tracking_no'= tracking_number)
print(mtdcdb.mtdxml(xml))

mtdcdb.initiateSession(user="mtdloadb", write=True)
fxml = open ("Shipping.xml","w")
fxml.write(mtdcdb.mtdxml(xml))
fxml.close()
mtdcdb.writeToDB(filename="Shipping.xml",user="mtdloadb")
