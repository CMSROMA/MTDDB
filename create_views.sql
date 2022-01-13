/* views are used to collect data to be shown by OMS:
   PARTSVIEW: list all parts in the DB
   CONSTRUCT_PROGRESS: for each part type shows the number of parts registered as a function of the
                       time
*/

CREATE VIEW PARTSVIEW AS
SELECT P.BARCODE, P.BATCH_NUMBER, K.DISPLAY_NAME, L.LOCATION_NAME, MA.MANUFACTURER_NAME,
       X.BATCH_INGOT_DATA, X.OPERATORCOMMENT
        FROM CMS_MTD_CORE_CONSTRUCT.PARTS P 
       	JOIN CMS_MTD_CORE_CONSTRUCT.KINDS_OF_PARTS K 
	ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID 
	JOIN CMS_MTD_CORE_MANAGEMNT.LOCATIONS L
	ON P.LOCATION_ID = L.LOCATION_ID
	JOIN CMS_MTD_CORE_CONSTRUCT.MANUFACTURERS MA
	ON MA.MANUFACTURER_ID = P.MANUFACTURER_ID
 	LEFT JOIN CMS_MTD_CORE_COND.COND_DATA_SETS CDS
	ON CDS.PART_ID = P.PART_ID
        LEFT JOIN CMS_MTD_TMING_COND.XTALREGISTRATION X
	ON X.CONDITION_DATA_SET_ID = CDS.CONDITION_DATA_SET_ID
	WHERE P.IS_RECORD_DELETED = 'F'
	ORDER BY K.DISPLAY_NAME, P.BARCODE;

CREATE VIEW CONSTRUCT_PROGRESS AS
SELECT dat.DT, COUNT(P.BARCODE) AS NPARTS, P.KIND_OF_PART_ID, K.DISPLAY_NAME
       FROM PARTS P JOIN KINDS_OF_PARTS K ON K.KIND_OF_PART_ID = P.KIND_OF_PART_ID,
       (SELECT TRUNC (SYSDATE - 30*ROWNUM) DT FROM DUAL 
        CONNECT BY ROWNUM < MONTHS_BETWEEN(SYSDATE, TO_DATE('2021-01-01', 'YYYY-MM-DD'))) dat
       WHERE trunc(P.RECORD_INSERTION_TIME) <= dat.DT AND P.IS_RECORD_DELETED = 'F' AND
       P.BARCODE NOT LIKE 'FK%'
       GROUP BY dat.DT, K.DISPLAY_NAME, P.KIND_OF_PART_ID ORDER BY P.KIND_OF_PART_ID, dat.DT;
