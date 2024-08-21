/* views are used to collect data to be shown by OMS:
   PARTSVIEW: list all parts in the DB
   CONSTRUCT_PROGRESS: for each part type shows the number of parts registered as a function of the
                       time
*/

CREATE VIEW ALLPARTS AS
SELECT P.BARCODE, P.SERIAL_NUMBER, P.BATCH_NUMBER, P.COMMENT_DESCRIPTION,
       K.DISPLAY_NAME, L.LOCATION_NAME, MA.MANUFACTURER_NAME,
       X.BATCH_INGOT_DATA, X.OPERATORCOMMENT, P.RECORD_INSERTION_USER,
       P.RECORD_INSERTION_TIME, KC.NAME, CR.RUN_TYPE,
       AC.DISPLAY_NAME AS ATTRIBUTE_NAME, PS.NAME AS ATTRIBUTE_VALUE
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
        LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PART_ATTR_LISTS AL
        ON AL.PART_ID = P.PART_ID
        LEFT JOIN CMS_MTD_CORE_ATTRIBUTE.POSITION_SCHEMAS PS
        ON PS.ATTRIBUTE_ID = AL.ATTRIBUTE_ID
        LEFT JOIN CMS_MTD_CORE_ATTRIBUTE.ATTR_BASES B
        ON B.ATTRIBUTE_ID = AL.ATTRIBUTE_ID
        LEFT JOIN CMS_MTD_CORE_ATTRIBUTE.ATTR_CATALOGS AC
        ON AC.ATTR_CATALOG_ID = B.ATTR_CATALOG_ID	
        WHERE P.IS_RECORD_DELETED = 'F'
        AND (CDS.IS_RECORD_DELETED = 'F' OR CDS.IS_RECORD_DELETED IS NULL)
        AND (KC.IS_RECORD_DELETED = 'F' OR KC.IS_RECORD_DELETED IS NULL)
/*        AND (AC.DISPLAY_NAME = 'Ingot Region' OR AC.DISPLAY_NAME IS NULL)	*/
        ORDER BY K.DISPLAY_NAME, P.BARCODE;

/*
CREATE VIEW PARTSVIEW AS
       SELECT * FROM ALLPARTS WHERE NAME = 'PART_REGISTRATION';
*/

CREATE VIEW PARTSVIEW AS
       SELECT A.*, LOWER(REGEXP_REPLACE(A.DISPLAY_NAME, ' .*', '')) AS GTYPE FROM ALLPARTS A INNER JOIN
       (SELECT BARCODE, MIN(ATTRIBUTE_NAME) AS ATTRIBUTE_NAME FROM ALLPARTS GROUP BY BARCODE) B
       ON A.BARCODE = B.BARCODE AND (A.ATTRIBUTE_NAME = B.ATTRIBUTE_NAME OR A.ATTRIBUTE_NAME IS NULL)
       WHERE NAME = 'PART_REGISTRATION' OR A.DISPLAY_NAME IN (SELECT DISTINCT K.DISPLAY_NAME FROM
       CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K JOIN CMS_MTD_CORE_CONSTRUCT.PART_TO_PART_RLTNSHPS R
       ON R.KIND_OF_PART_ID = K.KIND_OF_PART_ID WHERE A.NAME IS NULL);


CREATE VIEW CONSTRUCT_PROGRESS AS
SELECT TO_CHAR(TRUNC(SYSDATE), 'YYYY-MM-DD') AS DT, SYSDATE AS DTN, COUNT(P.BARCODE) AS NPARTS,
       P.KIND_OF_PART_ID, K.DISPLAY_NAME
       FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       WHERE trunc(P.RECORD_INSERTION_TIME) <= SYSDATE AND P.IS_RECORD_DELETED = 'F' AND
       P.BARCODE NOT LIKE 'FK%'
       GROUP BY K.DISPLAY_NAME, P.KIND_OF_PART_ID
UNION SELECT TO_CHAR(dat.DT, 'YYYY-MM-DD'), dat.DT AS DTN, COUNT(P.BARCODE) AS NPARTS,
      P.KIND_OF_PART_ID, K.DISPLAY_NAME
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
              FROM CMS_MTD_TMING_COND.LY_XTALK WHERE LY >= 0 GROUP BY TRUNC(ly/100,2)
       ) ORDER BY ID;

/*
	View to show xtalk data 
*/
CREATE VIEW XTALK AS
SELECT P.BARCODE, PP.BARCODE AS PARENT, L.LOCATION_NAME AS LOCATION, K.DISPLAY_NAME, KC.NAME,
       R.RUN_TYPE, R.RUN_NAME, R.RUN_BEGIN_TIMESTAMP, C.VERSION,
       R.INITIATED_BY_USER, X.RECORD_ID, X.CONDITION_DATA_SET_ID, X.CTR, X.CTR_NORM, X.TEMPERATURE,
       X.XTLEFT, X.XTRIGHT, (X.XTLEFT+X.XTRIGHT) AS XTALK, X.LY, X.LY_NORM, X.SIGMA_T,
       X.SIGMA_T_NORM FROM
       CMS_MTD_TMING_COND.LY_XTALK X
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS C ON C.CONDITION_DATA_SET_ID = X.CONDITION_DATA_SET_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = C.PART_ID
       JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L ON L.LOCATION_ID = P.LOCATION_ID	              
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = C.COND_RUN_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = C.KIND_OF_CONDITION_ID
       LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PHYSICAL_PARTS_TREE PT ON PT.PART_ID = P.PART_ID
       LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PARTS PP ON PP.PART_ID = PT.PART_PARENT_ID
       WHERE LY >= 0 and C.IS_RECORD_DELETED = 'F' AND R.IS_RECORD_DELETED = 'F';

CREATE VIEW PMT AS
SELECT P.BARCODE, K.DISPLAY_NAME, L.LOCATION_NAME AS LOCATION, KC.NAME, R.RUN_TYPE,
       R.RUN_NAME, R.RUN_BEGIN_TIMESTAMP, C.VERSION,
       R.INITIATED_BY_USER, X.B_RMS, X.B_3S_ASYM, X.B_2S_ASYM, X.LY_ABS,
       (X.LY_ABS/0.511/0.25) AS LO, X.LY_NORM, X.DECAY_TIME,
       (X.LY_ABS/0.511/0.25/X.DECAY_TIME) AS LO_OVER_DT
       FROM CMS_MTD_TMING_COND.LY_MEASUREMENT X
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS C ON C.CONDITION_DATA_SET_ID = X.CONDITION_DATA_SET_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = C.PART_ID
       JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L ON L.LOCATION_ID = P.LOCATION_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = C.COND_RUN_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = C.KIND_OF_CONDITION_ID
       WHERE LY_ABS >= 0 and C.IS_RECORD_DELETED = 'F' AND R.IS_RECORD_DELETED = 'F';

/*
CREATE VIEW SIPM_VENDOR_DATA AS
SELECT P.BARCODE, K.DISPLAY_NAME, L.LOCATION_NAME AS LOCATION, KC.NAME, R.RUN_TYPE,
       R.RUN_NAME, R.RUN_BEGIN_TIMESTAMP, C.VERSION,
       R.INITIATED_BY_USER, X.*
       FROM CMS_MTD_TMING_COND.SIPM_VENDOR_DATA X
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS C ON C.CONDITION_DATA_SET_ID = X.CONDITION_DATA_SET_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = C.PART_ID
       JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L ON L.LOCATION_ID = P.LOCATION_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = C.COND_RUN_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = C.KIND_OF_CONDITION_ID
       WHERE C.IS_RECORD_DELETED = 'F' AND R.IS_RECORD_DELETED = 'F';
*/

CREATE VIEW X3D AS
SELECT P.BARCODE,
       (CASE WHEN PP.BARCODE IS NULL THEN P.BARCODE ELSE PP.BARCODE END) AS PARENT,
       K.DISPLAY_NAME, L.LOCATION_NAME AS LOCATION, KC.NAME, R.RUN_TYPE,
       R.RUN_NAME, R.RUN_BEGIN_TIMESTAMP, C.VERSION,
       R.INITIATED_BY_USER, X.BARLENGTH, X.BARLENGTH_STD, X.LMAXVAR_LS, X.LMAXVAR_LN,
       GREATEST(X.LMAXVAR_LS, X.LMAXVAR_LN) AS LMAXVAR,
       X.LMAXVAR_LS_STD, X.LMAXVAR_LN_STD, X.L_MAX, X.L_MEAN, X.L_MEAN_STD, X.WMAXVAR_LO,
       X.WMAXVAR_LE, X.WMAXVAR_LO_STD, X.WMAXVAR_LE_STD, X.W_MAX, X.W_MEAN, X.W_MEAN_STD,
       X.TMAXVAR_FS, X.TMAXVAR_FS_STD, X.T_MAX, X.T_MEAN, X.T_MEAN_STD, X.L_MEAN_MITU,
       X.L_STD_MITU, X.W_MEAN_MITU, X.W_STD_MITU, X.T_MEAN_MITU, X.T_STD_MITU
       FROM CMS_MTD_TMING_COND.XTAL_DIMENSIONS X
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS C ON C.CONDITION_DATA_SET_ID = X.CONDITION_DATA_SET_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = C.PART_ID
       JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L ON L.LOCATION_ID = P.LOCATION_ID	       
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = C.COND_RUN_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = C.KIND_OF_CONDITION_ID
       LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PHYSICAL_PARTS_TREE PT ON PT.PART_ID = P.PART_ID
       LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PARTS PP ON
            (PP.PART_ID = PT.PART_PARENT_ID AND PP.BARCODE != 'CMS-MTD-ROOT')       
       WHERE C.IS_RECORD_DELETED = 'F' AND R.IS_RECORD_DELETED = 'F';

CREATE VIEW SIPMQCQA AS
SELECT P.BARCODE, 'F' AS IS_VENDOR,
       (CASE WHEN PP.BARCODE IS NULL THEN P.BARCODE ELSE PP.BARCODE END) AS PARENT,
       K.DISPLAY_NAME, L.LOCATION_NAME AS LOCATION, KC.NAME, R.RUN_TYPE,
       R.RUN_NAME, R.RUN_BEGIN_TIMESTAMP, C.VERSION,
       R.INITIATED_BY_USER, X.CHANNEL, X.DARKI3V*1e6 AS DARKI3VUA,
       X.DARKI5V*1e9 AS DARKI5VNA, X.RFWD, X.VBRRT, X.VBRCOLD
       FROM CMS_MTD_TMING_COND.SIPM_QC_QA X
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS C ON C.CONDITION_DATA_SET_ID = X.CONDITION_DATA_SET_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = C.PART_ID
       JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L ON L.LOCATION_ID = P.LOCATION_ID	       
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = C.COND_RUN_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = C.KIND_OF_CONDITION_ID
       LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PHYSICAL_PARTS_TREE PT ON PT.PART_ID = P.PART_ID
       LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PARTS PP ON
            (PP.PART_ID = PT.PART_PARENT_ID AND PP.BARCODE != 'CMS-MTD-ROOT')       
       WHERE C.IS_RECORD_DELETED = 'F' AND R.IS_RECORD_DELETED = 'F'
       UNION SELECT
       P.BARCODE, 'T' AS IS_VENDOR,
       (CASE WHEN PP.BARCODE IS NULL THEN P.BARCODE ELSE PP.BARCODE END) AS PARENT,
       K.DISPLAY_NAME, L.LOCATION_NAME AS LOCATION, KC.NAME, R.RUN_TYPE,
       R.RUN_NAME, R.RUN_BEGIN_TIMESTAMP, C.VERSION,
       R.INITIATED_BY_USER, X.CHANNEL, X.DARKI3V*1e6 AS DARKI3VUA,
       X.DARKI5V*1e9 AS DARKI5VNA, X.RFWD, X.VBRRT, NULL AS VBRCOLD
       FROM CMS_MTD_TMING_COND.SIPM_VENDOR_DATA X
       JOIN CMS_MTD_CORE_COND.COND_DATA_SETS C ON C.CONDITION_DATA_SET_ID = X.CONDITION_DATA_SET_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = C.PART_ID
       JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L ON L.LOCATION_ID = P.LOCATION_ID	       
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = C.COND_RUN_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = C.KIND_OF_CONDITION_ID
       LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PHYSICAL_PARTS_TREE PT ON PT.PART_ID = P.PART_ID
       LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PARTS PP ON
            (PP.PART_ID = PT.PART_PARENT_ID AND PP.BARCODE != 'CMS-MTD-ROOT')       
       WHERE C.IS_RECORD_DELETED = 'F' AND R.IS_RECORD_DELETED = 'F';

/*
CREATE VIEW TODO_LIST AS
SELECT DISTINCT REGEXP_REPLACE(P.BARCODE, '-.*', '') AS BARCODE, KC.NAME, KC.CATEGORY_NAME
       FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS KP ON KP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS CP ON CP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = CP.KIND_OF_CONDITION_ID
       LEFT JOIN CMS_MTD_CORE_COND.COND_DATA_SETS DS ON
       (DS.PART_ID = P.PART_ID AND DS.KIND_OF_CONDITION_ID = KC.KIND_OF_CONDITION_ID)
       WHERE P.IS_RECORD_DELETED = 'F' AND KC.IS_RECORD_DELETED = 'F' AND KP.IS_RECORD_DELETED = 'F'
       AND DS.CONDITION_DATA_SET_ID IS NULL
       ORDER BY BARCODE;

CREATE VIEW TODO_LIST AS
SELECT DISTINCT REGEXP_REPLACE(P.BARCODE, '-.*', '') AS BARCODE, KP.DISPLAY_NAME, KC.NAME, KC.CATEGORY_NAME
       FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS KP ON KP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS CP ON CP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = CP.KIND_OF_CONDITION_ID
       LEFT JOIN CMS_MTD_CORE_COND.COND_DATA_SETS DS ON
       (DS.PART_ID = P.PART_ID AND DS.KIND_OF_CONDITION_ID = KC.KIND_OF_CONDITION_ID)
       WHERE P.IS_RECORD_DELETED = 'F' AND KC.IS_RECORD_DELETED = 'F' AND KP.IS_RECORD_DELETED = 'F'
       AND DS.CONDITION_DATA_SET_ID IS NULL
       ORDER BY BARCODE;

SELECT DISTINCT REGEXP_REPLACE(SUBPART, '-.*', '') AS BARCODE, DISPLAY_NAME, NAME, CATEGORY_NAME,
       COUNT(*) AS COUNT FROM (
       SELECT DISTINCT P.BARCODE AS SUBPART, KP.DISPLAY_NAME, KC.NAME,
       	     KC.CATEGORY_NAME
       	     FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
       	     JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS KP ON KP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       	     JOIN CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS CP ON CP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
       	     JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = CP.KIND_OF_CONDITION_ID
       	     LEFT JOIN CMS_MTD_CORE_COND.COND_DATA_SETS DS ON
       	     (DS.PART_ID = P.PART_ID AND DS.KIND_OF_CONDITION_ID = KC.KIND_OF_CONDITION_ID)
       	     WHERE P.IS_RECORD_DELETED = 'F' AND KC.IS_RECORD_DELETED = 'F' AND KP.IS_RECORD_DELETED = 'F'
       	     AND DS.CONDITION_DATA_SET_ID IS NULL
       ) GROUP BY REGEXP_REPLACE(SUBPART, '-.*', ''), NAME, CATEGORY_NAME, DISPLAY_NAME;
*/

CREATE VIEW TODO_LIST AS
SELECT * FROM (
SELECT DISTINCT REGEXP_REPLACE(P.BARCODE, '-.*', '') AS BARCODE, K.DISPLAY_NAME,
        L.LOCATION_NAME AS LOCATION, KC.NAME, KC.CATEGORY_NAME AS CATEGORY,
        KP.DISPLAY_NAME AS PARENT_TYPE,
        (SELECT COUNT(S.CONDITION_DATA_SET_ID) FROM CMS_MTD_CORE_COND.COND_DATA_SETS S WHERE
         S.PART_ID = P.PART_ID AND S.KIND_OF_CONDITION_ID = KC.KIND_OF_CONDITION_ID AND
         S.VERSION != '0' AND KC.NAME != 'PART_REGISTRATION') AS COUNT
        FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
        JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON P.KIND_OF_PART_ID = K.KIND_OF_PART_ID
        JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L ON L.LOCATION_ID = P.LOCATION_ID
        JOIN CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS CP ON CP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
        JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID =
                CP.KIND_OF_CONDITION_ID
        LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PHYSICAL_PARTS_TREE PT ON PT.PART_ID = P.PART_ID
        LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PARTS PP ON PP.PART_ID = PT.PART_PARENT_ID
        LEFT JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS KP ON KP.KIND_OF_PART_ID = PP.KIND_OF_PART_ID
        WHERE KC.NAME != 'PART_REGISTRATION'
        ORDER BY COUNT, BARCODE, DISPLAY_NAME
) WHERE COUNT = 0;

/*
CREATE VIEW TODO_LIST AS
SELECT DISTINCT REGEXP_REPLACE(SUBPART, '-.*', '') AS BARCODE, DISPLAY_NAME, LOCATION_NAME AS LOCATION,
       NAME, CATEGORY_NAME,
       (SELECT DISPLAY_NAME FROM CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS KK 
        LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PARTS PP ON PP.KIND_OF_PART_ID = KK.KIND_OF_PART_ID 
	WHERE PP.BARCODE = REGEXP_REPLACE(SUBPART, '-.*', ''))
       AS PARENT_TYPE,
       COUNT(*) AS COUNT FROM (
       SELECT DISTINCT P.BARCODE AS SUBPART, KP.DISPLAY_NAME, KC.NAME, L.LOCATION_NAME,
             KC.CATEGORY_NAME
             FROM CMS_MTD_CORE_CONSTRUCT.PARTS P
	     JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L ON L.LOCATION_ID = P.LOCATION_ID
             JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS KP ON KP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
             JOIN CMS_MTD_CORE_COND.COND_TO_PART_RLTNSHPS CP ON CP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
             JOIN CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS KC ON KC.KIND_OF_CONDITION_ID = CP.KIND_OF_CONDITION_ID
             LEFT JOIN CMS_MTD_CORE_COND.COND_DATA_SETS DS ON
             (DS.PART_ID = P.PART_ID AND DS.KIND_OF_CONDITION_ID = KC.KIND_OF_CONDITION_ID)
             WHERE P.IS_RECORD_DELETED = 'F' AND KC.IS_RECORD_DELETED = 'F' AND KP.IS_RECORD_DELETED = 'F'
             AND (DS.CONDITION_DATA_SET_ID IS NULL OR (DS.VERSION = 0 OR DS.IS_RECORD_DELETED = 'T'))
       )
	GROUP BY REGEXP_REPLACE(SUBPART, '-.*', ''), NAME, CATEGORY_NAME, DISPLAY_NAME, LOCATION_NAME;
*/

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

CREATE VIEW PART_ATTRIBUTES AS
SELECT P.BARCODE, C.DISPLAY_NAME, P.NAME FROM
        CMS_MTD_CORE_ATTRIBUTE.POSITION_SCHEMAS P
        JOIN CMS_MTD_CORE_ATTRIBUTE.ATTR_BASES A ON A.ATTRIBUTE_ID = P.ATTRIBUTE_ID
        JOIN CMS_MTD_CORE_ATTRIBUTE.ATTR_CATALOGS C ON C.ATTR_CATALOG_ID = A.ATTR_CATALOG_ID
        JOIN CMS_MTD_CORE_CONSTRUCT.PART_ATTR_LISTS PA ON PA.ATTRIBUTE_ID = A.ATTRIBUTE_ID
        JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = PA.PART_ID;

CREATE VIEW SHIPPINGS AS
SELECT S.SHP_ID, S.SHP_COMPANY_NAME, S.SHP_TRACKING_NUMBER,
        (SELECT LOCATION_NAME FROM CMS_MTD_CORE_MANAGEMNT.LOCATIONS WHERE LOCATION_ID = S.SHP_FROM_LOCATION_ID)
        AS FROM_LOCATION,
        (SELECT LOCATION_NAME FROM CMS_MTD_CORE_MANAGEMNT.LOCATIONS WHERE LOCATION_ID = S.SHP_TO_LOCATION_ID)
        AS TO_LOCATION, S.SHP_STATUS, S.COMMENT_DESCRIPTION, S.SHP_DATE, S.SHP_PERSON, I.SHI_ID,
        P.BARCODE, KP.DISPLAY_NAME, I.SHI_RQI_ID
        FROM CMS_MTD_TMING_CONSTRUCT.SHIPMENTS S
        JOIN CMS_MTD_TMING_CONSTRUCT.SHIPMENT_ITEMS I ON I.SHI_SHP_ID = S.SHP_ID
        JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = I.SHI_PART_ID
        JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS KP ON KP.KIND_OF_PART_ID = P.KIND_OF_PART_ID
        ORDER BY SHP_DATE DESC, BARCODE;

CREATE VIEW IRRADIATIONS_RH AS
SELECT P.BARCODE, K.DISPLAY_NAME AS PART_TYPE,
        I.RADIATION_TYPE AS RADIATION_TYPE, DS.COND_RUN_ID,
        R.RUN_BEGIN_TIMESTAMP, R.RUN_NAME, R.RUN_TYPE, R.RUN_NUMBER, R.LOCATION,
        R.COMMENT_DESCRIPTION,
         I.DOSERATE, I.DOSE
        FROM CMS_MTD_TMING_COND.IRRADIATIONS I
        JOIN CMS_MTD_CORE_COND.COND_DATA_SETS DS ON DS.CONDITION_DATA_SET_ID = I.CONDITION_DATA_SET_ID
        JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = DS.PART_ID
        JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID
        JOIN CMS_MTD_CORE_COND.COND_RUNS R ON R.COND_RUN_ID = DS.COND_RUN_ID
        WHERE DS.IS_RECORD_DELETED = 'F' AND P.IS_RECORD_DELETED = 'F' AND R.IS_RECORD_DELETED = 'F'
        AND K.IS_RECORD_DELETED = 'F';

CREATE VIEW PARTS_TREE AS
SELECT P.BARCODE, PC.BARCODE AS CHILD, K.DISPLAY_NAME AS TYPE, 
       LOWER(REGEXP_REPLACE(K.DISPLAY_NAME, ' .*', '')) AS GTYPE, C.DISPLAY_NAME AS ATTRIBUTE_NAME, 
       P.NAME AS ATTRIBUTE_VALUE
       FROM CMS_MTD_CORE_CONSTRUCT.PHYSICAL_PARTS_TREE PT
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS P ON P.PART_ID = PT.PART_PARENT_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.PARTS PC ON PC.PART_ID = PT.PART_ID
       JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = PC.KIND_OF_PART_ID
       LEFT JOIN CMS_MTD_CORE_CONSTRUCT.PART_ATTR_LISTS A ON A.PART_ID = PC.PART_ID
       LEFT JOIN CMS_MTD_CORE_ATTRIBUTE.POSITION_SCHEMAS P ON P.ATTRIBUTE_ID = A.ATTRIBUTE_ID
       LEFT JOIN CMS_MTD_CORE_ATTRIBUTE.ATTR_BASES B ON B.ATTRIBUTE_ID = A.ATTRIBUTE_ID
       LEFT JOIN CMS_MTD_CORE_ATTRIBUTE.ATTR_CATALOGS C ON C.ATTR_CATALOG_ID = B.ATTR_CATALOG_ID;


