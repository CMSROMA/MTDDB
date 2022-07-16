#!/usr/bin/python3

import pyqtgui as gui
import sys

class BTLModule(gui.ConstructionHelper):
    def __init__(self):
        super().__init__('BTLModule Assembly', 'Building a BTL module')

    def restoreLastConfiguration(self):
        lines = self.loadLastConfiguration()
        if len(lines) >= 3:
            self.leftSiPM.setText(lines[0].replace('\n', ''))
            self.leftSiPM.displayText()
            self.LYSOMatrix.setText(lines[1].replace('\n', ''))
            self.LYSOMatrix.displayText()
            self.rightSiPM.setText(lines[2].replace('\n', ''))
            self.rightSiPM.displayText()
        
    def build(self):
        self.leftSiPM = self.loadImage('Left SiPM Barcode', self.entry_layout, 'SiPM.jpg')
        self.LYSOMatrix = self.loadImage('LYSOMatrix Barcode', self.entry_layout, 'LYSMatrix.png')
        self.rightSiPM = self.loadImage('Right SiPM Barcode', self.entry_layout, 'SiPM.jpg')        

    def clear(self):
        super().clear()
        self.leftSiPM.clear()
        self.LYSOMatrix.clear()
        self.rightSiPM.clear()

    def save(self):
        super().save()
        f = open('.lastsession', 'w')
        if self.leftSiPM.text() == None:
            self.leftSiPM.setText('')
        if self.rightSiPM.text() == None:
            self.rightSiPM.setText('')
        if self.LYSOMatrix.text() == None:
            self.LYSOMatrix.setText('')
        f.write(self.leftSiPM.text() + '\n')
        f.write(self.LYSOMatrix.text() + '\n')
        f.write(self.rightSiPM.text() + '\n')
        f.close()
        print('[INFO] Session saved')

    def xml(self):
        bcodes = [self.superPartBarcode.text(), self.leftSiPM.text(),
                  self.LYSOMatrix.text(), self.rightSiPM.text()]
        super().xml(bcodes, '.btlmodule.xml')
        return

app = gui.QApplication(sys.argv)

window = BTLModule()
window.show()

app.exec()
