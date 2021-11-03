from lxml import etree
import sys
import os, signal
import getpass
import subprocess
import re
import time
from datetime import datetime
import cx_Oracle
import time 
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging
from datetime import date
from datetime import datetime
from pwd import getpwuid

os.environ['TNS_ADMIN'] = '/home/dev/dbloader'
fname = os.environ['TNS_ADMIN'] + '/logs/' + str(date.today()) + '.log'

observer = Observer()

class ExampleHandler(FileSystemEventHandler):
    def on_created(self, event): # when file is created
        # do something, eg. call your function to process the image
        now = str(datetime.now())
        logging.basicConfig(filename = fname, level = logging.INFO)
        logging.info(f'{now}: Got event for file {event.src_path}')
        xml_owner = getpwuid(stat(event.src_path).st_uid).pw_name
        logging.info(f'                 owned by {xml_owner}')
        if event.src_path == '/home/dev/dbloader/xml/STOP':
            logging.info(f'{now}: Exiting gently')
            os.remove(event.src_path)
            os.kill(os.getpid(), signal.SIGSTOP)
        else:
            X = mtdxml()
            X.debug()
            X.getxml(event.src_path)
            X.dump()
            X.openConnection('CMS_MTD_CORE_COND', '<THE-DB-PASSWORD>')
            X.insertRun()
            X.getxml(event.src_path)
            X.insertDataset()
            done_path = str(event.src_path).replace('/xml/', '/history/')
            print(f'Moving XML to {done_path}')
            os.rename(event.src_path, done_path)

class mtdxml:
    debug_ = False
    def debug(self, debug = True):
        self.debug_ = debug
    def getxml(self, filename):
        self.tree = etree.parse(filename)
        self.event_types = ("start", "end")
        self.parser = etree.XMLPullParser(self.event_types)
        self.parser.feed(self.tostring())
    def openConnection(self, user, password, tns = 'INT2R'): 
        self.conn = cx_Oracle.connect(user = user,
                                      password = password,
                                      dsn = tns)
        self.cursor = self.conn.cursor()
    def insertDataset(self, fake = True):
        version = kind_of_part = barcode = name = extension_table_name = ''
        isDataset = False
        isData = False
        data = {}
        for action, elem in self.parser.read_events():
            if action in self.event_types:
                if elem.tag == 'NAME':
                    name = elem.text
                if elem.tag == 'EXTENSION_TABLE_NAME':
                    extension_table_name = elem.text
                if elem.tag == 'DATA_SET' and action == 'start':
                    isDataset = True
                if elem.tag == 'DATA_SET' and action == 'end':
                    isDataset = False
                if isDataset:
                    if elem.tag == 'VERSION':
                        version = elem.text
                    if elem.tag == 'KIND_OF_PART':
                        kind_of_part = elem.text
                    if elem.tag == 'BARCODE':
                        barcode = elem.text
                    if elem.tag == 'DATA' and action == 'end':
                        if self.debug_:
                            print(data)
                        self.dataset2db(barcode, name, extension_table_name, version, data, fake)
                        isData = False
                    if isData:
                        data.update({elem.tag: elem.text})
                    if elem.tag == 'DATA' and action == 'start':
                        data = {}
                        isData = True                        
    def dataset2db(self, barcode, name, extension_table_name, version, data, fake = True):
        if self.debug_:
            logging.info(f'Recording a data set for part {barcode}')
            logging.info(f'                  cond_run_id {self.cond_run_id}')
            logging.info(f'                    condition {name}')
            logging.info(f'                      version {version}')
            logging.info(f'                   table_name {extension_table_name}')
            for recordColumn, recordValue in data.items():
                logging.info(f'{recordColumn:>29s} {recordValue}')
        pars = [ ('F', barcode, self.cond_run_id, name, name, version) ]
        sql = 'INSERT INTO CMS_MTD_CORE_COND.COND_DATA_SETS ' + \
              '(IS_RECORD_DELETED, PART_ID, COND_RUN_ID, KIND_OF_CONDITION_ID, ' + \
              'EXTENSION_TABLE_NAME, VERSION) ' + \
              'VALUES (:1, (SELECT PART_ID FROM CMS_MTD_CORE_CONSTRUCT.PARTS WHERE BARCODE = :2), ' + \
              ':3, (SELECT KIND_OF_CONDITION_ID FROM CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS ' + \
              'WHERE NAME = :4), (SELECT EXTENSION_TABLE_NAME FROM ' + \
              'CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE NAME = :5), :6)'
        if self.debug_:
            print(sql)
            print(pars)
        self.conn.begin()
        self.cursor.executemany(sql, pars)
        sql = 'SELECT CONDITION_DATA_SET_ID FROM ' + \
              'CMS_MTD_CORE_COND.COND_DATA_SETS WHERE ' + \
              'IS_RECORD_DELETED = :1 AND ' + \
              'PART_ID = (SELECT PART_ID FROM CMS_MTD_CORE_CONSTRUCT.PARTS WHERE BARCODE = :2) AND ' + \
              'COND_RUN_ID = :3 AND ' + \
              'KIND_OF_CONDITION_ID = (SELECT KIND_OF_CONDITION_ID FROM ' + \
              'CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE NAME = :4) AND ' + \
              'EXTENSION_TABLE_NAME = (SELECT EXTENSION_TABLE_NAME FROM ' + \
              'CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE NAME = :4) AND ' + \
              'VERSION = :5'
        if self.debug_:
            print(sql)
        self.cursor.execute(sql, pars[0])
        condition_data_set_id = self.cursor.fetchone()[0]
        sql = 'SELECT EXTENSION_TABLE_NAME FROM CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE ' + \
              'NAME = :1'
        self.cursor.execute(sql, [(name) ])
        extension_table_name = self.cursor.fetchone()[0]
        # the next instructions are needed to circumvent the problem of not knowing how to 
        # set the primary key
        sql = f'SELECT MAX(RECORD_ID) FROM CMS_MTD_TMING_COND.{extension_table_name}'
        self.cursor.execute(sql)
        record_id = self.cursor.fetchone()[0] + 1
        # when solved, we should remove RECORD_ID from the fields to be set with the insert statement
        sql = f'INSERT INTO CMS_MTD_TMING_COND.{extension_table_name} (RECORD_ID, CONDITION_DATA_SET_ID, '
        n = len(data)
        i = 0
        values = [ str(record_id), str(condition_data_set_id) ] 
        for column, value in data.items():
            values.append(value)
            sql += column
            i += 1
            if i < n:
                sql += ', '
        sql += ') VALUES ('
        for i in range(n + 2):
            j = i + 1
            sql += f':{j}'
            if i < n + 1:
                sql += ', '
        sql += ')';
        if self.debug_:
            print(sql)
            print(values)
        dataValues = [ (values) ]
        self.cursor.executemany(sql, dataValues) 
        if fake:
            logging.info(f'fake insert: CONDITION_DATA_SET_ID = {condition_data_set_id}')
            self.conn.rollback()
        else:
            self.conn.commit()
    def insertRun(self, fake = True):
        run_name = run_type = run_begin_timestamp = run_end_timestamp = ''
        for action, elem in self.parser.read_events():
            if action in self.event_types:
                if elem.tag == 'RUN_NAME':
                    run_name = elem.text
                elif elem.tag == 'RUN_TYPE':
                    run_type = elem.text
                elif elem.tag == 'RUN_BEGIN_TIMESTAMP':
                    run_begin_timestamp = elem.text
                elif elem.tag == 'RUN_END_TIMESTAMP':
                    run_end_timestamp = elem.text
        rows = [ ('F', run_name, run_type, run_begin_timestamp, run_end_timestamp) ]
        sql = 'INSERT INTO CMS_MTD_CORE_COND.COND_RUNS ' + \
            '(IS_RECORD_DELETED, RUN_NAME, RUN_TYPE, RUN_BEGIN_TIMESTAMP, RUN_END_TIMESTAMP) ' + \
            'VALUES (:1, :2, :3, TO_DATE(:4, \'YYYY-MM-DD HH24:MI:SS\'), ' + \
            'TO_DATE(:5, \'YYYY-MM-DD HH24:MI:SS\'))'
        if self.debug_:
            print(sql)
        self.conn.begin()
        self.cursor.executemany(sql, rows)
        sql = 'SELECT COND_RUN_ID FROM CMS_MTD_CORE_COND.COND_RUNS WHERE ' + \
            'IS_RECORD_DELETED = :1 AND ' +\
            f'RUN_NAME = :2 AND RUN_TYPE = :3 AND ' + \
            f'RUN_BEGIN_TIMESTAMP = TO_DATE(:4, \'YYYY-MM-DD HH24:MI:SS\') AND ' + \
            f'RUN_END_TIMESTAMP = TO_DATE(:5, \'YYYY-MM-DD HH24:MI:SS\')'
        if self.debug_:
            print(sql)
        self.cursor.execute(sql, rows[0])
        self.cond_run_id = self.cursor.fetchone()[0]
        if fake:
            logging.info(f'fake insert: COND_RUN_ID = {self.cond_run_id}')
            self.conn.rollback()
        else:
            self.conn.commit()
    def tostring(self):
        return etree.tostring(self.tree.getroot()).decode()
    def dump(self):
        print(self.tostring())

event_handler = ExampleHandler() # create event handler
observer.schedule(event_handler, path='/home/dev/dbloader/xml')
observer.start()

# sleep until keyboard interrupt, then stop + rejoin the observer
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()

observer.join()
        
