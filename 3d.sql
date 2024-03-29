


/*
        To be run as CMS_MTD_CORE_COND
*/

INSERT INTO CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS (NAME, IS_RECORD_DELETED, EXTENSION_TABLE_NAME, COMMENT_DESCRIPTION) VALUES ('XTAL DIMENSIONS', 'F', 'XTAL_DIMENSIONS', '3D measurement of a crystal');
/*
	To be run as CMS_MTD_TMING_COND
*/

DROP TABLE CMS_MTD_TMING_COND.XTAL_DIMENSIONS CASCADE CONSTRAINTS;

CREATE TABLE CMS_MTD_TMING_COND.XTAL_DIMENSIONS
(
  RECORD_ID                NUMBER(38)	NOT NULL,
  CONDITION_DATA_SET_ID	   NUMBER(38)	NOT NULL,
  BARLENGTH	NUMBER,
  BARLENGTH_STD	NUMBER,
  LMAXVAR_LS	NUMBER,
  LMAXVAR_LN	NUMBER,
  LMAXVAR_LS_STD NUMBER,
  LMAXVAR_LN_STD NUMBER,
  L_MAX	NUMBER,
  L_MEAN	NUMBER,
  L_MEAN_STD	NUMBER,
  WMAXVAR_LO	NUMBER,
  WMAXVAR_LE	NUMBER,
  WMAXVAR_LO_STD    NUMBER,
  WMAXVAR_LE_STD    NUMBER,  
  W_MAX	NUMBER,
  W_MEAN	NUMBER,
  W_MEAN_STD	NUMBER,
  TMAXVAR_FS	NUMBER,
  TMAXVAR_FS_STD	NUMBER,
  T_MAX	NUMBER,
  T_MEAN	NUMBER,
  T_MEAN_STD	NUMBER,
  L_MEAN_MITU	NUMBER,
  L_STD_MITU	NUMBER,
  W_MEAN_MITU	NUMBER,
  W_STD_MITU	NUMBER,
  T_MEAN_MITU	NUMBER,
  T_STD_MITU	NUMBER
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

CREATE UNIQUE INDEX CMS_MTD_TMING_COND.XTL_DMNSNS_PK ON CMS_MTD_TMING_COND.XTAL_DIMENSIONS (RECORD_ID);

ALTER TABLE CMS_MTD_TMING_COND.XTAL_DIMENSIONS ADD (
  CONSTRAINT XTL_DMNSNS_PK
  PRIMARY KEY
  (RECORD_ID)
  USING INDEX CMS_MTD_TMING_COND.XTL_DMNSNS_PK
  ENABLE VALIDATE);


CREATE INDEX CMS_MTD_TMING_COND.XTL_DMNSNS_FK_I ON CMS_MTD_TMING_COND.XTAL_DIMENSIONS (CONDITION_DATA_SET_ID);

ALTER TABLE CMS_MTD_TMING_COND.XTAL_DIMENSIONS ADD (
  CONSTRAINT XTL_DMNSNS_FK 
  FOREIGN KEY (CONDITION_DATA_SET_ID) 
  REFERENCES CMS_MTD_CORE_COND.COND_DATA_SETS (CONDITION_DATA_SET_ID)
  ENABLE VALIDATE);

GRANT SELECT ON CMS_MTD_TMING_COND.XTAL_DIMENSIONS TO CMS_MTD_TMING_CONDITION_READER;

GRANT SELECT ON CMS_MTD_TMING_COND.XTAL_DIMENSIONS TO PUBLIC;

GRANT INSERT ON CMS_MTD_TMING_COND.XTAL_DIMENSIONS TO CMS_MTD_CORE_CONDITION_WRITER;
GRANT UPDATE ON CMS_MTD_TMING_COND.XTAL_DIMENSIONS TO CMS_MTD_CORE_CONDITION_WRITER;
GRANT INSERT ON CMS_MTD_TMING_COND.XTAL_DIMENSIONS TO CMS_MTD_PRTYP_TMING_WRT;
GRANT UPDATE ON CMS_MTD_TMING_COND.XTAL_DIMENSIONS TO CMS_MTD_PRTYP_TMING_WRT;

INSERT INTO CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS
  (KIND_OF_PART_ID, KIND_OF_CONDITION_ID, DISPLAY_NAME, IS_RECORD_DELETED)
  VALUES ((SELECT KIND_OF_PART_ID FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS WHERE
           DISPLAY_NAME = 'singleCrystal #1' AND IS_RECORD_DELETED = 'F'),
           (SELECT KIND_OF_CONDITION_ID FROM
           CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE EXTENSION_TABLE_NAME = 'XTAL_DIMENSIONS' AND 
           NAME = 'XTAL DIMENSIONS'), '3D measurement of a crystal for singleCrystal #1', 'F'); 
INSERT INTO CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS
  (KIND_OF_PART_ID, KIND_OF_CONDITION_ID, DISPLAY_NAME, IS_RECORD_DELETED)
  VALUES ((SELECT KIND_OF_PART_ID FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS WHERE
           DISPLAY_NAME = 'singleCrystal #2' AND IS_RECORD_DELETED = 'F'),
           (SELECT KIND_OF_CONDITION_ID FROM
           CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE EXTENSION_TABLE_NAME = 'XTAL_DIMENSIONS' AND 
           NAME = 'XTAL DIMENSIONS'), '3D measurement of a crystal for singleCrystal #2', 'F'); 
INSERT INTO CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS
  (KIND_OF_PART_ID, KIND_OF_CONDITION_ID, DISPLAY_NAME, IS_RECORD_DELETED)
  VALUES ((SELECT KIND_OF_PART_ID FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS WHERE
           DISPLAY_NAME = 'singleCrystal #3' AND IS_RECORD_DELETED = 'F'),
           (SELECT KIND_OF_CONDITION_ID FROM
           CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE EXTENSION_TABLE_NAME = 'XTAL_DIMENSIONS' AND 
           NAME = 'XTAL DIMENSIONS'), '3D measurement of a crystal for singleCrystal #3', 'F'); 
INSERT INTO CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS
  (KIND_OF_PART_ID, KIND_OF_CONDITION_ID, DISPLAY_NAME, IS_RECORD_DELETED)
  VALUES ((SELECT KIND_OF_PART_ID FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS WHERE
           DISPLAY_NAME = 'singleCrystal #1' AND IS_RECORD_DELETED = 'F'),
           (SELECT KIND_OF_CONDITION_ID FROM
           CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE EXTENSION_TABLE_NAME = 'XTAL_DIMENSIONS' AND 
           NAME = 'XTAL DIMENSIONS'), '3D measurement of a crystal for singleCrystal #1', 'F'); 
INSERT INTO CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS
  (KIND_OF_PART_ID, KIND_OF_CONDITION_ID, DISPLAY_NAME, IS_RECORD_DELETED)
  VALUES ((SELECT KIND_OF_PART_ID FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS WHERE
           DISPLAY_NAME = 'singleBarCrystal' AND IS_RECORD_DELETED = 'F'),
           (SELECT KIND_OF_CONDITION_ID FROM
           CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE EXTENSION_TABLE_NAME = 'XTAL_DIMENSIONS' AND 
           NAME = 'XTAL DIMENSIONS'), '3D measurement of a crystal for singleBarCrystal', 'F'); 
INSERT INTO CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS
  (KIND_OF_PART_ID, KIND_OF_CONDITION_ID, DISPLAY_NAME, IS_RECORD_DELETED)
  VALUES ((SELECT KIND_OF_PART_ID FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS WHERE
           DISPLAY_NAME = 'LYSOMatrix #3' AND IS_RECORD_DELETED = 'F'),
           (SELECT KIND_OF_CONDITION_ID FROM
           CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE EXTENSION_TABLE_NAME = 'XTAL_DIMENSIONS' AND 
           NAME = 'XTAL DIMENSIONS'), '3D measurement of a crystal for LYSOMatrix #3', 'F'); 
INSERT INTO CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS
  (KIND_OF_PART_ID, KIND_OF_CONDITION_ID, DISPLAY_NAME, IS_RECORD_DELETED)
  VALUES ((SELECT KIND_OF_PART_ID FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS WHERE
           DISPLAY_NAME = 'LYSOMatrix #2' AND IS_RECORD_DELETED = 'F'),
           (SELECT KIND_OF_CONDITION_ID FROM
           CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE EXTENSION_TABLE_NAME = 'XTAL_DIMENSIONS' AND 
           NAME = 'XTAL DIMENSIONS'), '3D measurement of a crystal for LYSOMatrix #2', 'F'); 
INSERT INTO CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS
  (KIND_OF_PART_ID, KIND_OF_CONDITION_ID, DISPLAY_NAME, IS_RECORD_DELETED)
  VALUES ((SELECT KIND_OF_PART_ID FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS WHERE
           DISPLAY_NAME = 'LYSOMatrix #1' AND IS_RECORD_DELETED = 'F'),
           (SELECT KIND_OF_CONDITION_ID FROM
           CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS WHERE EXTENSION_TABLE_NAME = 'XTAL_DIMENSIONS' AND 
           NAME = 'XTAL DIMENSIONS'), '3D measurement of a crystal for LYSOMatrix #1', 'F'); 
