/* views are used to collect data to be shown by OMS:
   PARTSVIEW: list all parts in the DB
   CONSTRUCT_PROGRESS: for each part type shows the number of parts registered as a function of the
                       time
*/

CREATE VIEW PARTSVIEW AS
SELECT P.BARCODE, P.BATCH_NUMBER, K.DISPLAY_NAME, L.LOCATION_NAME, MA.MANUFACTURER_NAME,
       X.BATCH_INGOT_DATA, X.OPERATORCOMMENT, CDS.RECORD_INSERTION_USER,
       CDS.RECORD_INSERTION_TIME, KC.NAME, CR.RUN_TYPE
        FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
        JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K
        ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
        LEFT JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L
        ON P.LOCATION_ID = L.LOCATION_ID
        LEFT JOIN CMS_MTD_CORE_CONSTRUCT.MANUFACTURERS MA
        ON MA.MANUFACTURER_ID = P.MANUFACTURER_ID
        LEFT JOIN CMS_MTD_CORE_COND.COND_DATA_SETS CDS
        ON CDS.PART_ID = P.PART_ID
        LEFT JOIN CMS_MTD_TMING_COND.PART_REGISTRATION X
        ON X.CONDITION_DATA_SET_ID = CDS.CONDITION_DATA_SET_ID
        LEFT JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC
        ON KC.KIND_OF_CONDITION_ID = CDS.KIND_OF_CONDITION_ID
        LEFT JOIN CMS_MTD_CORE_COND.COND_RUNS CR
        ON CR.COND_RUN_ID = CDS.COND_RUN_ID
        WHERE P.IS_RECORD_DELETED = 'F'
        AND (CDS.IS_RECORD_DELETED = 'F' OR CDS.IS_RECORD_DELETED IS NULL)
        AND (KC.IS_RECORD_DELETED = 'F' OR KC.IS_RECORD_DELETED IS NULL)
        AND (KC.NAME = 'XTALREGISTRATION')
        ORDER BY K.DISPLAY_NAME, P.BARCODE;

CREATE VIEW CONSTRUCT_PROGRESS AS
SELECT TO_CHAR(TRUNC(SYSDATE), 'YYYY-MM-DD') AS DT, COUNT(P.BARCODE) AS NPARTS,
       P.KIND_OF_PART_ID, K.DISPLAY_NAME
       FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       WHERE trunc(P.RECORD_INSERTION_TIME) <= SYSDATE AND P.IS_RECORD_DELETED = 'F' AND
       P.BARCODE NOT LIKE 'FK%'
       GROUP BY K.DISPLAY_NAME, P.KIND_OF_PART_ID
UNION SELECT TO_CHAR(dat.DT, 'YYYY-MM-DD'), COUNT(P.BARCODE) AS NPARTS, P.KIND_OF_PART_ID, K.DISPLAY_NAME
       FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID,
       (SELECT TRUNC(SYSDATE - 30*ROWNUM, 'MM') DT FROM DUAL
        CONNECT BY ROWNUM < MONTHS_BETWEEN(SYSDATE, TO_DATE('2021-01-01', 'YYYY-MM-DD'))) dat
       WHERE trunc(P.RECORD_INSERTION_TIME) <= dat.DT AND P.IS_RECORD_DELETED = 'F' AND
       P.BARCODE NOT LIKE 'FK%'
       GROUP BY dat.DT, K.DISPLAY_NAME, P.KIND_OF_PART_ID
       ORDER BY KIND_OF_PART_ID, DT;

CREATE VIEW PARTS_IN_DB AS
SELECT CASE WHEN DISPLAY_NAME != 'singleBarCrystal' THEN
         DISPLAY_NAME
       ELSE
         'singleBarCrystal X 15'
       END AS DISPLAY_NAME,
       CASE WHEN DISPLAY_NAME != 'singleBarCrystal' THEN
         SUM(NPARTS)
       ELSE
         SUM(NPARTS/16)
       END AS NPARTS FROM CONSTRUCT_PROGRESS GROUP BY DISPLAY_NAME;

/* 
   A histogram is a table with the following columns
   id: the bins unique identifier for a given category
   category: a string identifying the data collected
   x: the bins' values
   n: the counts
   In order to obtain the id, one takes the value to be histogrammed and
   divide it by a number C, taking a limited number of digits. The corresponding
   x is obtained as the id times the number C
*/
CREATE VIEW HISTO AS
SELECT ID, 'LY' AS CATEGORY, ID*100 as X, N FROM (
       SELECT TO_CHAR(TRUNC(ly/100,2)) AS ID, COUNT(*) AS N
              FROM CMS_MTD_TMING_COND.LY_XTALK GROUP BY TRUNC(ly/100,2)
       ) ORDER BY ID;

/*
	View to show xtalk data 
*/
CREATE VIEW XTALK AS
SELECT P.BARCODE, K.DISPLAY_NAME, KC.NAME, R.RUN_TYPE, R.RUN_NAME, R.RUN_BEGIN_TIMESTAMP,
       R.INITIATED_BY_USER, X.RECORD_ID, X.CONDITION_DATA_SET_ID, X.CTR, X.CTR_NORM, X.TEMPERATURE,
       X.XTLEFT, X.XTRIGHT, X.LY, X.LY_NORM, X.SIGMA_T, X.SIGMA_T_NORM FROM
       CMS_MTD_TMING_COND.LY_XTALK X
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS C ON C.CONDITION_DATA_SET_ID = X.CONDITION_DATA_SET_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = C.PART_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = C.COND_RUN_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = C.KIND_OF_CONDITION_ID;

CREATE VIEW PMT AS
SELECT P.BARCODE, K.DISPLAY_NAME, KC.NAME, R.RUN_TYPE, R.RUN_NAME, R.RUN_BEGIN_TIMESTAMP,
       R.INITIATED_BY_USER, X.B_RMS, X.B_3S_ASYM, X.B_2S_ASYM, X.LY_ABS, X.LY_NORM, X.DECAY_TIME
       FROM CMS_MTD_TMING_COND.LY_MEASUREMENT X
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS C ON C.CONDITION_DATA_SET_ID = X.CONDITION_DATA_SET_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = C.PART_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = C.COND_RUN_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = C.KIND_OF_CONDITION_ID;

CREATE VIEW X3D AS
SELECT P.BARCODE, K.DISPLAY_NAME, KC.NAME, R.RUN_TYPE, R.RUN_NAME, R.RUN_BEGIN_TIMESTAMP,
       R.INITIATED_BY_USER, X.BARLENGTH, X.BARLENGTH_STD, X.LMAXVAR_LS, X.LMAXVAR_LN,
       X.LMAXVAR_LS_STD, X.LMAXVAR_LN_STD, X.L_MAX, X.L_MEAN, X.L_MEAN_STD, X.WMAXVAR_LO,
       X.WMAXVAR_LE, X.WMAXVAR_LO_STD, X.WMAXVAR_LE_STD, X.W_MAX, X.W_MEAN, X.W_MEAN_STD,
       X.TMAXVAR_FS, X.TMAXVAR_FS_STD, X.T_MAX, X.T_MEAN, X.T_MEAN_STD, X.L_MEAN_MITU,
       X.L_STD_MITU, X.W_MEAN_MITU, X.W_STD_MITU, X.T_MEAN_MITU, X.T_STD_MITU
       FROM CMS_MTD_TMING_COND.XTAL_DIMENSIONS X
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS C ON C.CONDITION_DATA_SET_ID = X.CONDITION_DATA_SET_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = C.PART_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = C.COND_RUN_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = C.KIND_OF_CONDITION_ID;

CREATE VIEW TODO_LIST AS
SELECT DISTINCT T.BARCODE, T.NAME, T.CATEGORY_NAME FROM (
  SELECT DISTINCT P.BARCODE, P.PART_ID, P.KIND_OF_PART_ID, P.IS_RECORD_DELETED, KC.NAME,
  	 	  KC.KIND_OF_CONDITION_ID, KC.CATEGORY_NAME
  	 FROM CMS_MTD_CORE_CONSTRUCT.PARTS P, CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC
  	 WHERE P.IS_RECORD_DELETED = 'F' AND KC.IS_RECORD_DELETED = 'F') T
  JOIN CMS_MTD_CORE_COND.COND_DATA_SETS DS ON
  (DS.PART_ID = T.PART_ID AND NOT (DS.KIND_OF_CONDITION_ID = T.KIND_OF_CONDITION_ID))
  JOIN CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS KPR ON	   
  (KPR.KIND_OF_CONDITION_ID = T.KIND_OF_CONDITION_ID AND KPR.KIND_OF_PART_ID = T.KIND_OF_PART_ID)
  JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS KP ON KP.KIND_OF_PART_ID = T.KIND_OF_PART_ID
  WHERE T.IS_RECORD_DELETED = 'F' AND KP.DISPLAY_NAME NOT LIKE 'singleBarCrystal'
  ORDER BY T.BARCODE;

CREATE VIEW FIND_ACTIVITIES AS
SELECT P.BARCODE, KP.DISPLAY_NAME AS TYPE, A.KIND_OF_CONDITION_ID, K.NAME, A.DISPLAY_NAME,
       D.EXTENSION_TABLE_NAME, R.RUN_NAME, R.RUN_TYPE, R.RUN_NUMBER, R.INITIATED_BY_USER,
       R.LOCATION, R.RUN_BEGIN_TIMESTAMP, R.RUN_END_TIMESTAMP
       FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS KP ON KP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS A ON A.KIND_OF_PART_ID = KP.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS K ON K.KIND_OF_CONDITION_ID = A.KIND_OF_CONDITION_ID
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS D ON
	    (D.PART_ID = P.PART_ID AND D.KIND_OF_CONDITION_ID = A.KIND_OF_CONDITION_ID)
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = D.COND_RUN_ID
       WHERE A.IS_RECORD_DELETED = 'F' AND K.IS_RECORD_DELETED = 'F' AND D.IS_RECORD_DELETED = 'F'
	     AND R.IS_RECORD_DELETED = 'F';
