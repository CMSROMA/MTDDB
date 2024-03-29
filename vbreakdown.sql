


/*
        To be run as CMS_MTD_CORE_COND
*/

INSERT INTO CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS (NAME, IS_RECORD_DELETED, EXTENSION_TABLE_NAME, COMMENT_DESCRIPTION) VALUES ('SiPMBreakdownVoltage', 'F', 'SiPMBreakdownVoltage', 'A table containing data collected by the manufacturer from which the V_BD is obtained');
/*
	To be run as CMS_MTD_TMING_COND
*/

DROP TABLE CMS_MTD_TMING_COND.SiPMBreakdownVoltage CASCADE CONSTRAINTS;

CREATE TABLE CMS_MTD_TMING_COND.SiPMBreakdownVoltage
(
  RECORD_ID                NUMBER(38)	NOT NULL,
  CONDITION_DATA_SET_ID	   NUMBER(38)	NOT NULL,
  VBreakDown NUMBER NOT NULL,
  V1 NUMBER,
  I1 number,
  V2 number,
  I2 number	
)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          10M
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE;

CREATE UNIQUE INDEX CMS_MTD_TMING_COND.SPMBrkdwnVltg_PK ON CMS_MTD_TMING_COND.SiPMBreakdownVoltage (RECORD_ID);

ALTER TABLE CMS_MTD_TMING_COND.SiPMBreakdownVoltage ADD (
  CONSTRAINT SPMBrkdwnVltg_PK
  PRIMARY KEY
  (RECORD_ID)
  USING INDEX CMS_MTD_TMING_COND.SPMBrkdwnVltg_PK
  ENABLE VALIDATE);


CREATE INDEX CMS_MTD_TMING_COND.SPMBrkdwnVltg_FK_I ON CMS_MTD_TMING_COND.SiPMBreakdownVoltage (CONDITION_DATA_SET_ID);

ALTER TABLE CMS_MTD_TMING_COND.SiPMBreakdownVoltage ADD (
  CONSTRAINT SPMBrkdwnVltg_FK 
  FOREIGN KEY (CONDITION_DATA_SET_ID) 
  REFERENCES CMS_MTD_CORE_COND.COND_DATA_SETS (CONDITION_DATA_SET_ID)
  ENABLE VALIDATE);

GRANT SELECT ON CMS_MTD_TMING_COND.SiPMBreakdownVoltage TO CMS_MTD_TMING_CONDITION_READER;

GRANT SELECT ON CMS_MTD_TMING_COND.SiPMBreakdownVoltage TO PUBLIC;

GRANT INSERT ON CMS_MTD_TMING_COND.SiPMBreakdownVoltage TO CMS_MTD_CORE_CONDITION_WRITER;
GRANT UPDATE ON CMS_MTD_TMING_COND.SiPMBreakdownVoltage TO CMS_MTD_CORE_CONDITION_WRITER;
GRANT INSERT ON CMS_MTD_TMING_COND.SiPMBreakdownVoltage TO CMS_MTD_PRTYP_TMING_WRT;
GRANT UPDATE ON CMS_MTD_TMING_COND.SiPMBreakdownVoltage TO CMS_MTD_PRTYP_TMING_WRT;

INSERT INTO CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS
  (KIND_OF_PART_ID, KIND_OF_CONDITION_ID, DISPLAY_NAME, IS_RECORD_DELETED)
  VALUES ((SELECT KIND_OF_PART_ID FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS WHERE
           DISPLAY_NAME = 'SiPM' AND IS_RECORD_DELETED = 'F'),
           (SELECT KIND_OF_CONDITION_ID FROM
           CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE EXTENSION_TABLE_NAME = 'SiPMBreakdownVoltage' AND 
           NAME = 'SiPMBreakdownVoltage'), 'Breakdown Voltage', 'F'); 
