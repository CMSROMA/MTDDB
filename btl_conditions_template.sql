DROP TABLE CMS_MTD_TMING_COND.TEMPLATE_NAME CASCADE CONSTRAINTS;

CREATE TABLE CMS_MTD_TMING_COND.TEMPLATE_NAME
(
  RECORD_ID                NUMBER(38)	NOT NULL,
  CONDITION_DATA_SET_ID	   NUMBER(38)	NOT NULL,
  TEMPLATE_COLUMNS
)
TABLESPACE CMS_MTD_PROCERNIT_DATA
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

CREATE UNIQUE INDEX CMS_MTD_TMING_COND.TEMPLATE_NAME_PK ON CMS_MTD_TMING_COND.TEMPLATE_NAME
(RECORD_ID)
LOGGING
TABLESPACE CMS_MTD_TMING_COND
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          10M
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           );

ALTER TABLE CMS_MTD_TMING_COND.TEMPLATE_NAME ADD (
  CONSTRAINT TEMPLATE_NAME_PK
  PRIMARY KEY
  (RECORD_ID)
  USING INDEX CMS_MTD_TMING_COND.TEMPLATE_NAME_PK
  ENABLE VALIDATE);


CREATE INDEX CMS_MTD_TMING_COND.TEMPLATE_SHRTNAME_FK_I ON CMS_MTD_TMING_COND.TEMPLATE_NAME
(CONDITION_DATA_SET_ID)
LOGGING
TABLESPACE CMS_MTD_TMING_COND
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          10M
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           );

ALTER TABLE CMS_MTD_TMING_COND.TEMPLATE_NAME ADD (
  CONSTRAINT TEMPLATE_NAME_FK 
  FOREIGN KEY (CONDITION_DATA_SET_ID) 
  REFERENCES CMS_MTD_CORE_COND.COND_DATA_SETS (CONDITION_DATA_SET_ID)
  ENABLE VALIDATE);

GRANT SELECT ON CMS_MTD_TMING_COND.TEMPLATE_NAME TO CMS_MTD_TMING_CONDITION_READER;

GRANT SELECT ON CMS_MTD_TMING_COND.TEMPLATE_NAME TO PUBLIC;
