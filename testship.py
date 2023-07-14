from mtdConstructionDBTools import mtdcdb

#barcodes = range(842,852,1)
#barcodes = [f'PRE{bc:010d}' for bc in barcodes] 

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Insert last 6 digits of the barcod of every shipped part
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#barcodes = [100027,100028,100035,100036,200031,200032,200033,200034,200047,200048,300011,300012] #UVA
#barcodes = [100029,100030,100033,100034,200035,200036,200037,200038,200030,200053,300015,300016] #PKU
barcodes = [100020,100021,100022,100032,200043,200044,200045,200046,300009,300010] #Milano

barcodes = [f'3211{str(bc).zfill(10)}' for bc in barcodes] # auto-format barcode
#tracking_number = '5060629991' # PLEASE insert shipping tracking number!!!!!
tracking_number = '' # PLEASE insert shipping tracking number!!!!! #PKU
shipping_company = 'Private shipping' # PLEASE add shipping company name: DHL for shipping to USA and CHINA UPS for shipping to Italy
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# BAC Location: Bicocca, Virginia, Caltech, PKU (Peking University
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

xml = mtdcdb.xml2ship(barcodes, from_institution = 'Roma', from_location = 'Roma',
                      to_institution = 'Bicocca', to_location = 'Bicocca', tracking_no = tracking_number, company = shipping_company)
print(mtdcdb.mtdxml(xml))

mtdcdb.initiateSession(user="mtdloadb", write=True)
fxml = open ("Shipping.xml","w")
fxml.write(mtdcdb.mtdxml(xml))
fxml.close()
mtdcdb.writeToDB(filename="Shipping.xml",user="mtdloadb")
