#!/usr/bin/python3

import sys
import os
import re

import PyQt6.QtCore as QtCore
from PyQt6.QtGui import *
from PyQt6.QtWidgets import *

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.loadConfiguration()
        font = QFont('Arial', 12)
        self.setFont(font)
        self.userWidgets = []

    def loadLastConfiguration(self):
        f = None
        try:
            f = open('.lastsession')
        except:
            print('[INFO] No previously saved session found')
        lines = []
        if f != None:
            lines = f.readlines()
        return lines

    def loadConfiguration(self):
        self.location = 'Undefined'
        try:
            f = open('.gui.conf')
        except:
            print('[INFO] Cannot find local configuration')
        else:
            conflines = f.readlines()
            for line in conflines:
                res = [i for i in conflines if 'location' in i]
                if len(res) > 0:
                    self.location = re.split(' *= *', res[0])[1]
        print(f'[INFO] You are supposed to be in {self.location}')

class ConstructionHelper(MainWindow):
    def __init__(self, title, toplabel):
        super().__init__()

        self.layout = QVBoxLayout()
        self.button_layout = QHBoxLayout()

        self.setTitle(title)
        self.topFrameLabel(toplabel)

        self.entry_layout = QHBoxLayout()        
        self.layout.addLayout(self.entry_layout)
        self.build()
        
        self.saveButton = self.createButton('Save', self.save, color = 'green')
        self.clearButton = self.createButton('Clear', self.clear, color = 'yellow')
        self.xmlButton = self.createButton('Generate XML', self.xml, color = 'orange')
        self.submitButton = self.createButton('Submit', self.submit, color = 'red')

        self.layout.addLayout(self.button_layout)

        widget = QWidget()
        widget.setLayout(self.layout)
        self.setCentralWidget(widget)
        
        self.restoreLastConfiguration()

    def restoreLastConfiguration(self):
        lines = self.loadLastConfiguration()
        if len(lines) >= len(self.userWidgets):
            i = 0
            for w in self.userWidgets:
                w.setText(lines[i].replace('\n', ''))
                w.displayText()
                i += 1
                
    def createButton(self, title, callback, color = 'white'):
        button = QPushButton(title)
        button.clicked.connect(callback)
        button.setStyleSheet(f'background-color: {color}')        
        self.button_layout.addWidget(button)
        return button

    def createLineEdit(self, label, layout, image = None):
        innerlayout = QVBoxLayout()

        if image != None:
            img = QLabel(self)
            pixmap = QPixmap(image)
            img.setPixmap(pixmap.scaled(300, 300, QtCore.Qt.AspectRatioMode.KeepAspectRatio))
            img.setAlignment(QtCore.Qt.AlignmentFlag.AlignHCenter)            
            innerlayout.addWidget(img)

        le = QLineEdit()
        le.setMaxLength(13)
        le.setFixedWidth(13 * 12)
        le.setAlignment(QtCore.Qt.AlignmentFlag.AlignHCenter)        
        innerlayout.addWidget(le, alignment = QtCore.Qt.AlignmentFlag.AlignHCenter)
        self.userWidgets.append(le)
        self.label = QLabel(self)
        self.label.setText(label)
        self.label.setAlignment(QtCore.Qt.AlignmentFlag.AlignHCenter)
        innerlayout.addWidget(self.label)
        layout.addLayout(innerlayout)
        return le
                
    def save(self):
        print('[INFO] Saving...')
        f = open('.lastsession', 'w')        
        for w in self.userWidgets:
            if w.text() == None:
                w.setText('')
            f.write(w.text() + '\n')
        f.close()
        
    def clear(self):
        print('[INFO] Clear input...')
        for w in self.userWidgets:
            w.clear()

    def xml(self, bcodes = [], filename = '.none'):
        print('[INFO] Generating the XML...')
        f = open(filename, 'w')
        f.write(f"<?xml version='1.0' encoding='UTF-8' standalone='yes'?>\n")
        f.write(f'<ROOT encoding="xmlns:xsi=http://www.w3.org/2001/XMLSchema-instance">\n')
        f.write(f'  <PARTS>\n')
        f.write(f'    <PART mode="auto">\n')
        f.write(f'      <BARCODE>{bcodes[0]}</BARCODE>\n')
        f.write(f'      <RECORD_INSERTION_USER>{os.getlogin()}</RECORD_INSERTION_USER>\n')
        f.write(f'      <LOCATION>{self.location}</LOCATION>\n')
        f.write(f'      <CHILDREN>\n')
        bcodes.pop(0)
        for bcode in bcodes:
            f.write(self.part(bcode))
        f.write(f'      </CHILDREN>\n')
        f.write(f'    </PART>\n')        
        f.write(f'  </PARTS>\n')        
        f.write(f'</ROOT>\n') 
        f.close()

    def part(self, barcode):
        xml = '        <PART>\n'
        xml += f'          <BARCODE>{barcode}</BARCODE>\n'
        xml += '        </PART>\n'
        return xml
        
    def submit(self):
        self.xml()
        print('[INFO] Submitting to DB...')
        self.submitButton.setEnabled(False)
        self.check()

    def check(self):
        return False
    
    def setTitle(self, title):
        self.setWindowTitle(title)

    def topFrameLabel(self, label):
        self.label = QLabel(self)
        lfnt = QFont('Arial', 24)
        lfnt.setBold(True)
        self.label.setFont(QFont(lfnt))
#        self.label.setText(label + '\N{BLACK QUESTION MARK ORNAMENT}')
        self.label.setText(label)
        self.label.adjustSize()
        self.label.setAlignment(QtCore.Qt.AlignmentFlag.AlignHCenter)
        self.layout.addWidget(self.label)
        top_layout = QHBoxLayout()
        login = QLabel(self)
        login.setText('Assembling BTLModule by user ' + os.getlogin())
        login.setAlignment(QtCore.Qt.AlignmentFlag.AlignTop)
        top_layout.addWidget(login)
        self.superPartBarcode = self.createLineEdit('BTLModule barcode', top_layout)
        top_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignAbsolute)            
        self.layout.addLayout(top_layout)

    def loadImage(self, title, layout, name):
        return self.createLineEdit(title, layout, '.mtdicons/' + name)
        
    def build(self):
        return

