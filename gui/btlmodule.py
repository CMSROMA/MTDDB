#!/usr/bin/python3

import pyqtgui as gui
import sys

class BTLModule(gui.ConstructionHelper):
    def __init__(self):
        super().__init__('BTLModule Assembly', 'Building a BTL module')

    def build(self):
        self.leftSiPM = self.loadImage('Left SiPM Barcode', self.entry_layout, 'SiPM.jpg')
        self.LYSOMatrix = self.loadImage('LYSOMatrix Barcode', self.entry_layout, 'LYSMatrix.png')
        self.rightSiPM = self.loadImage('Right SiPM Barcode', self.entry_layout, 'SiPM.jpg')        

    def xml(self):
        bcodes = [self.superPartBarcode.text(), self.leftSiPM.text(),
                  self.LYSOMatrix.text(), self.rightSiPM.text()]
        super().xml(bcodes, '.btlmodule.xml')
        return

app = gui.QApplication(sys.argv)

window = BTLModule()
window.show()

app.exec()
