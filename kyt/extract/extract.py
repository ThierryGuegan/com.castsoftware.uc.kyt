import os
import sys
import collections
import psycopg2
import json
import logging
logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.INFO
)

import common.timewatch
import common.config



class Acc:
    _v = None

    def sto( aExpr ):
        Acc._v = aExpr
        return Acc._v

    def v():
        retVal = Acc._v
        Acc._v = None
        return retVal


##== ------------------------------------------------------------------------
def postgresExecuteQuery( aConn, aQuery, aIsDDL=False ):
    retVal = []
    vCurs = aConn.cursor()
    #logger.info( "query:\n{}\n------".format(aQuery) )
    vRes = vCurs.execute( aQuery )
    #print( "-- begin - query ------\n{0}\n--  end  - query ------".format(aQuery), file=sys.stderr )
    if not aIsDDL:
        retVal = vCurs.fetchall()
    return retVal

TQueryConfig = collections.namedtuple("TQueryConfig","countH countQ selectH selectQ")


##== List of all transactions -----------------------------------------------
## Number and list of transactions
# 0: local schema name
queryListOfAllTransactions_base="""\
SELECT
    O.object_language_name	AS "LanguageName",
	O.object_id 			AS "TransactionId",
	O.object_name			AS "TransactionName",
	O.object_type_str		AS "TransactionType",
	O.object_fullname		AS "TransactionFullName"
FROM
    {0}.cdt_objects		        O
    JOIN {0}.dss_transaction	T	ON T.form_id=O.object_id
ORDER BY
	O.object_language_name, O.object_name, O.object_fullname, O.object_id
"""

configAllTransactions=TQueryConfig(
	countH	= "#NbOfAllTransactions",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryListOfAllTransactions_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#0/A:LanguageName|1/B:TransactionId|2/C:TransactionName|3/D:TransactionType|4/E:TransactionFullname",
	selectQ	= queryListOfAllTransactions_base
)



##== List of transactions ---------------------------------------------------
# {0}: local schema, {1}: criteria value, {2}: criteria:={id|name|fullname}
queryListOfTransactions_byX_base="""\
SELECT
    O.object_language_name	AS "LanguageName",
	O.object_id 			AS "TransactionId",
	O.object_name			AS "TransactionName",
	O.object_type_str		AS "TransactionType",
	O.object_fullname		AS "TransactionFullName"
FROM
    {0}.cdt_objects		        O
    JOIN {0}.dss_transaction	T	ON T.form_id=O.object_id
WHERE 1=1
	AND O.object_{2} = {1}
ORDER BY
	O.object_language_name, O.object_name, O.object_fullname, O.object_id
"""

configTransactions=TQueryConfig(
	countH	= "#NbOfTransactions",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryListOfTransactions_byX_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#0/A:LanguageName|1/B:TransactionId|2/C:TransactionName|3/D:TransactionType|4/E:TransactionFullname",
	selectQ	= queryListOfTransactions_byX_base
)



##== List of all transactions -----------------------------------------------
# 0: local schema name
headersListOfTransactions="#LanguageName|TransactionId|TransactionName|TransactionType|TransactionFullname"
queryListOfTransactions="""\
SELECT
    O.object_language_name	AS "LanguageName",
	O.object_id 			AS "TransactionId",
	O.object_name			AS "TransactionName",
	O.object_type_str		AS "TransactionType",
	O.object_fullname		AS "TransactionFullName"
FROM
    {0}.cdt_objects		        O
    JOIN {0}.dss_transaction	T	ON T.form_id=O.object_id
ORDER BY
	O.object_language_name, O.object_name, O.object_fullname, O.object_id
"""



##== List of transaction roots ----------------------------------------------
# TODO



##== List of transaction objects --------------------------------------------
## 2c - Given a transaction name/id, count of transaction objects
# 0: local schema name, 1: transaction name, 2: { "id" | "name" | "fullname" } 
queryTransactionObjectsByX_base_DEPRECATED="""\
SELECT DISTINCT
    OT.object_id AS "TransactionId", OT.object_name AS "TransactionName",  OT.object_type_str "TransactionType", OT.object_fullname AS "TransactionFullname",
    A.object_id AS "ArtifactId", A.object_name AS "ArtifactName", A.object_type_Str "ArtifactType", A.object_fullname AS "ArtifactFullname"
FROM
	{0}.cdt_objects		OT
	JOIN {0}.dss_transaction	T	ON T.form_id = OT.object_id
	JOIN {0}.dss_objects	O2	ON O2.object_id = T.object_id
	JOIN {0}.dss_links		L	ON L.previous_object_id = O2.object_id
	JOIN {0}.cdt_objects 	A	ON A.object_id = L.next_object_id
WHERE 1=1
	AND OT.object_{2} = {1}
"""

queryTransactionObjectsByX_base="""\
SELECT DISTINCT
    OT.object_id AS "TransactionId", OT.object_name AS "TransactionName",  OT.object_type_str "TransactionType", OT.object_fullname AS "TransactionFullname",
    A.object_id AS "ArtifactId", A.object_name AS "ArtifactName", A.object_type_Str "ArtifactType", A.object_fullname AS "ArtifactFullname"
FROM
	{0}.cdt_objects		OT
	JOIN {0}.dss_transaction	T	ON T.form_id = OT.object_id
	JOIN {0}.dss_objects	O2	ON O2.object_id = T.object_id
    --
	JOIN {0}.dss_transactiondetails		DT	ON DT.object_id = O2.object_id
	JOIN {0}.cdt_objects 	A	ON A.object_id = DT.child_id
WHERE 1=1
	AND OT.object_{2} = {1}
"""

configTransactionObjects=TQueryConfig(
	countH	= "#NbOfTransactionObjects",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryTransactionObjectsByX_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#TransactionId|TransactionName|TransactionType|TransactionFullname|ObjectId|ObjectName|ObjectType|ObjectFullname",
	selectQ	= queryTransactionObjectsByX_base
)



##== Transaction links ------------------------------------------------------
## 2e - Given a transaction name/id, count of transaction links
queryTransactionLinks_byX_base_DEPRECATED="""\
SELECT
    OT.object_id AS TransactionId,
    O1.object_id, O2.object_id,
    OT.object_name AS TransactionName, OT.object_fullname AS TransactionFullname,
    O1.object_name, O1.object_fullname,
    O2.object_name, O2.object_fullname,
    LT.link_type_name, LT.link_type_description
FROM
	-- Retrieve transaction
	{0}.cdt_objects		OT
	JOIN {0}.dss_transaction	T	ON T.form_id = OT.object_id
	JOIN {0}.dss_objects	OT2	ON OT2.object_id = T.object_id
	--
	-- retrieve potential callers (ie object involved in transaction)
	JOIN {0}.dss_links		L1	ON L1.previous_object_id = OT2.object_id
	JOIN {0}.cdt_objects	O1	ON O1.object_id = L1.next_object_id
	--
	-- retrieve calleds (object called by the caller, and involved in transaction)
	JOIN {0}.ctv_links		TL	ON TL.caller_id=O1.object_id
	JOIN {0}.cdt_objects	O2	ON O2.object_id = TL.called_id
	JOIN {0}.dss_links		L2	ON L2.previous_object_id = OT2.object_id
								AND L2.next_object_id=O2.object_id
	LEFT JOIN {0}.ctv_link_types    LT	ON LT.link_type_hi = TL.link_type_hi
										AND LT.link_type_lo = TL.link_type_lo
										AND LT.link_type_hi2 = TL.link_type_hi2
										AND LT.link_type_lo2 = TL.link_type_lo2
WHERE
	OT.object_{2} = {1}
	AND OT2.object_type_id = 30002
"""

queryTransactionLinks_byX_base="""\
SELECT
    OT.object_id AS TransactionId,
    O1.object_id, O2.object_id,
    OT.object_name AS TransactionName, OT.object_fullname AS TransactionFullname,
    O1.object_name, O1.object_fullname,
    O2.object_name, O2.object_fullname,
    LT.link_type_name, LT.link_type_description
FROM
	-- Retrieve transaction
	{0}.cdt_objects		OT
	JOIN {0}.dss_transaction	T	ON T.form_id = OT.object_id
	JOIN {0}.dss_objects	OT2	ON OT2.object_id = T.object_id
	--
	-- retrieve potential callers (ie object involved in transaction)
	----JOIN sicas_local.dss_links		L1	ON L1.previous_object_id = OT2.object_id
	----JOIN sicas_local.cdt_objects	O1	ON O1.object_id = L1.next_object_id
	JOIN {0}.dss_transactiondetails		DT	ON DT.object_id = OT2.object_id
	JOIN {0}.cdt_objects 	O1	ON O1.object_id = DT.child_id
	--
	-- retrieve calleds (object called by the caller, and involved in transaction)
	JOIN {0}.ctv_links		TL	ON TL.caller_id=O1.object_id
	JOIN {0}.cdt_objects	O2	ON O2.object_id = TL.called_id
	----JOIN sicas_local.dss_links		L2	ON L2.previous_object_id = OT2.object_id
	----							AND L2.next_object_id=O2.object_id
	JOIN {0}.dss_transactiondetails		DT2	ON  DT2.object_id = OT2.object_id
												AND DT2.child_id=O2.object_id
	LEFT JOIN {0}.ctv_link_types    LT	ON LT.link_type_hi = TL.link_type_hi
										AND LT.link_type_lo = TL.link_type_lo
										AND LT.link_type_hi2 = TL.link_type_hi2
										AND LT.link_type_lo2 = TL.link_type_lo2
WHERE
	OT.object_{2} = {1}
	AND OT2.object_type_id = 30002
"""

configTransactionLinks=TQueryConfig(
	countH	= "#NbOfTransactionLinks",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryTransactionLinks_byX_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#TransactionId|Object1Id|Object2Id|TransactionName|TransactionFullname|Object1Name|Object1Fullname|Object2Name|Object2Fullname|LinkTypeName|LinkTypeDesc",
	selectQ	= queryTransactionLinks_byX_base
)



##== Transaction roots
# {0}: local schema name, {1}: transaction name
# Result set: { "TransactionId", "TransactionName", "RootId", "RootName", "RootFullname", "RootType" }
headersTransactionRoots="#TransactionId|TransactionName|RootObjectId|RootObjectName|RootObjectFullname|RootObjectType"
queryTransactionRoots_byX_base="""\
SELECT
	OT.object_id AS "TransactionId", OT.object_name AS "TransactionName",
	RO.object_id AS "RootId", RO.object_name AS "RootName", RO.object_fullname AS "RootFullname", RO.object_type_str AS "RootType",
	'end'
FROM
	{0}.cdt_objects			OT
	JOIN {0}.dss_transaction		T	ON T.form_id = OT.object_id
	JOIN {0}.dss_objects		OT2	ON OT2.object_id = T.object_id
	JOIN {0}.dss_transactionroots	R	ON R.object_id=T.object_id
	-- for root object information to retrieve in selec
	JOIN {0}.cdt_objects		RO	ON RO.object_id=R.root_id
WHERE
	OT.object_{2} = {1}
	AND OT2.object_type_id = 30002
"""



##== List of transaction endpoints ------------------------------------------
##== Transaction objects that are endpoints of a transaction
# {0}: local schema name, {1}: transaction name
queryListOfTransactionEndpoints_byX_base_DEPRECATED="""\
SELECT
	OT.object_id AS "TransactionId", OT.object_name AS "TransactionName",
	O.object_id AS "EndpointId", O.object_name AS "EndpointName",
	O.object_type_Str "EndpointType", O.object_fullname AS "EndpointFullname",
	OT.object_type_str "TransactionType",
	OT.object_fullname AS "TransactionFullname"
FROM
	{0}.dss_transaction			T
	JOIN {0}.cdt_objects		OT	ON OT.object_id = T.form_id
	JOIN {0}.dss_objects		OT2	ON OT2.object_id = T.object_id
	JOIN {0}.dss_links			L	ON L.previous_object_id = OT2.object_id
	JOIN {0}.cdt_objects		O	ON O.object_id = L.next_object_id
	JOIN {0}.fp_dataendpoints	E	ON E.object_id=O.object_id
WHERE 1=1
    AND OT.object_{2} = {1}
"""

queryListOfTransactionEndpoints_byX_base="""\
SELECT
	OT.object_id AS "TransactionId", OT.object_name AS "TransactionName",
	O.object_id AS "EndpointId", O.object_name AS "EndpointName",
	O.object_type_Str "EndpointType", O.object_fullname AS "EndpointFullname",
	OT.object_type_str "TransactionType",
	OT.object_fullname AS "TransactionFullname"
FROM
	{0}.dss_transaction			T
	JOIN {0}.cdt_objects		OT	ON OT.object_id = T.form_id
	JOIN {0}.dss_objects		OT2	ON OT2.object_id = T.object_id
	--
	JOIN {0}.dss_transactiondetails		DT	ON DT.object_id = OT2.object_id
    JOIN {0}.cdt_objects		O	ON O.object_id = DT.child_id
	JOIN {0}.fp_dataendpoints	E	ON E.object_id=O.object_id
WHERE 1=1
    AND OT.object_{2} = {1}
"""


configTransactionEndpoints=TQueryConfig(
	countH	= "#NbOfTransactionEndpoints",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryListOfTransactionEndpoints_byX_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#0/A:TransactionId|1/B:TransactionName|2/C:EndpointId|3/D:EndpointName|4/E:EndpointType[5/F:EndpointFullname|6/F:TransactionType|7/G:transactionFullname",
	selectQ	= queryListOfTransactionEndpoints_byX_base
)



##== Transaction objects with critical violations ---------------------------
queryTransactionObjectsWithCV_byX_base_DEPRECATED="""\
SELECT DISTINCT
    L2C.site_object_id     AS "LocalObjectId",
    O.object_id            AS "Central0bjectId",
    R.metric_id-1          AS "MetricId",
    -- critical
    CASE WHEN (
        SELECT MAX(sT.metric_critical)
        FROM   {1}.dss_metric_histo_tree sT
        WHERE  sT.metric_id=R.metric_id-1 AND sT.snapshot_id=R.snapshot_id
    ) = 1 THEN 'Yes'
          ELSE 'No'
    END                    AS "Critical",
    Q.b_criterion_id,
    Q.t_criterion_id,
    O.object_name          AS "ObjectName",
    O.object_full_name     AS "ObjectFullname",
    Q.b_criterion_name,
    Q.t_criterion_name,
    Q.metric_name          AS "MetricName",
    --
    R.snapshot_id          AS "SnapshotId", 
    O.object_description   AS "ObjectDescription"
FROM
    -- Objects of transaction -> LOCAL_O
    {0}.cdt_objects                OT
    JOIN {0}.dss_transaction       T	   ON T.form_id = OT.object_id
    JOIN {0}.dss_objects           OT2     ON OT2.object_id = T.object_id
    JOIN {0}.dss_links             L       ON L.previous_object_id = OT2.object_id
    JOIN {0}.cdt_objects           LOCAL_O ON LOCAL_O.object_id = L.next_object_id	
    --
    -- translate local object ids into central object ids
    JOIN {1}.dss_translation_table L2C	   ON L2C.site_object_id = LOCAL_O.object_id
    JOIN {1}.dss_metric_results    R	   ON R.object_id = L2C.object_id
    -- for information to be output in select part
    JOIN {1}.dss_objects           O	   ON O.object_id=L2C.object_id
    JOIN {1}.csv_quality_tree      Q	   ON Q.metric_id=R.metric_id-1
WHERE 1=1
    -- objects in transaction
    AND OT.object_{3} = {2}
    AND OT2.object_type_id = 30002		
    --
    AND R.snapshot_id = ( SELECT MAX(S.snapshot_id) FROM {1}.dss_snapshots S )
    --
    AND R.metric_value_index=1
    AND Q.b_criterion_id = 60017
    AND 1=(
        SELECT MAX(iT.metric_critical)
        FROM   {1}.dss_metric_histo_tree iT
        WHERE  iT.metric_id=R.metric_id-1 AND iT.snapshot_id=R.snapshot_id
    )
"""

queryTransactionObjectsWithCV_byX_base="""\
SELECT DISTINCT
    L2C.site_object_id     AS "LocalObjectId",
    O.object_id            AS "Central0bjectId",
    R.metric_id-1          AS "MetricId",
    -- critical
    CASE WHEN (
        SELECT MAX(sT.metric_critical)
        FROM   {1}.dss_metric_histo_tree sT
        WHERE  sT.metric_id=R.metric_id-1 AND sT.snapshot_id=R.snapshot_id
    ) = 1 THEN 'Yes'
          ELSE 'No'
    END                    AS "Critical",
    Q.b_criterion_id,
    Q.t_criterion_id,
    O.object_name          AS "ObjectName",
    O.object_full_name     AS "ObjectFullname",
    Q.b_criterion_name,
    Q.t_criterion_name,
    Q.metric_name          AS "MetricName",
    --
    R.snapshot_id          AS "SnapshotId", 
    O.object_description   AS "ObjectDescription"
FROM
    -- Objects of transaction -> LOCAL_O
    {0}.cdt_objects                OT
    JOIN {0}.dss_transaction       T	   ON T.form_id = OT.object_id
    JOIN {0}.dss_objects           OT2     ON OT2.object_id = T.object_id
    JOIN {0}.dss_links             L       ON L.previous_object_id = OT2.object_id
    JOIN {0}.cdt_objects           LOCAL_O ON LOCAL_O.object_id = L.next_object_id	
    --
    -- translate local object ids into central object ids
    JOIN {1}.dss_translation_table L2C	   ON L2C.site_object_id = LOCAL_O.object_id
    JOIN {1}.dss_metric_results    R	   ON R.object_id = L2C.object_id
    -- for information to be output in select part
    JOIN {1}.dss_objects           O	   ON O.object_id=L2C.object_id
    JOIN {1}.csv_quality_tree      Q	   ON Q.metric_id=R.metric_id-1
	JOIN {1}.dss_metric_histo_tree	HT	ON HT.metric_id=R.metric_id-1
													AND HT.snapshot_id=LOCAL_O.snapshot_id
WHERE 1=1
    -- objects in transaction
    AND OT.object_{3} = {2}
    AND OT2.object_type_id = 30002		
    --
    AND R.snapshot_id = ( SELECT MAX(S.snapshot_id) FROM {1}.dss_snapshots S )
    --
    AND R.metric_value_index=1
    AND Q.b_criterion_id = 60017
    AND 1=HT.metric_critical
    --AND 1=(
    --    SELECT MAX(iT.metric_critical)
    --    FROM   {1}.dss_metric_histo_tree iT
    --    WHERE  iT.metric_id=R.metric_id-1 AND iT.snapshot_id=R.snapshot_id
    --)
"""
configTransactionObjectsWithCV=TQueryConfig(
	countH	= "#NbOfTransactionObjectsWithVC",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryTransactionObjectsWithCV_byX_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#LocalObjectId|CentralObjectId|MetricId|isCritical|"\
    	"BCriterionId|TCriterionId|ObjectName|ObjectFullname|BCriterionName|TCriterionName|MetricName|"\
    	"SnapshotId|ObjectDesc",
	selectQ	= queryTransactionObjectsWithCV_byX_base
)


### Version with temporary table
queryTransactionObjectsWithCriticalViolations_byX_base_DEPRECATED="""\
CREATE TEMPORARY TABLE temp_object_ids (
  object_id int4, object_name varchar(1000), object_fullname varchar(1000), object_central_id int4,
  object1_id int4, object1_name varchar(1000), object1_fullname varchar(1000),
  object2_id int4, object2_name varchar(1000), object2_fullname varchar(1000),
  valstr varchar(255), valint int4,
  val1str varchar(255), val1int int4,
  val2str varchar(255), val2int int4,
  snapshot_id int4
)
;
INSERT INTO temp_object_ids
SELECT
    OA.object_id       "ObjectId",				-- -> object_id
    OA.object_name     "ObjectName",			-- -> object_name
    OA.object_fullname	"ObjectFullname",		-- -> object_fullname
    L2C.object_id,										-- -> object_central_id
    --
    OT.object_id		"TransactionId",				-- -> object1_id
    OT.object_name     "TransactionName",				-- -> object1_name
    OT.object_fullname	"TransactionFullname",			-- -> object1_fullname
    --
    NULL, NULL,	NULL,								-- -> object_id, object2_name, object_fullname								
    OT.object_type_str "TransactionType", NULL,		-- -> valstr, valint
    OA.object_type_Str "ArtifactType", NULL,		-- -> val1str, val1int
    'Transaction-object' 	"EntryKind", NULL,			-- -> val2str, val2int
    NULL											-- -> sanpshot_id
FROM
    {0}.cdt_objects				OT
    JOIN {0}.dss_transaction	TR	ON TR.form_id = OT.object_id
    JOIN {0}.dss_objects		OT2	ON OT2.object_id = TR.object_id
    JOIN {0}.dss_links			L ON L.previous_object_id = OT2.object_id
    JOIN {0}.cdt_objects 		OA	ON OA.object_id = L.next_object_id
    --
    JOIN {1}.dss_translation_table	L2C	ON L2C.site_object_id = OA.object_id
WHERE 1=1
    AND OT.object_{3} = {2}
    AND OT2.object_type_id = 30002
;
UPDATE temp_object_ids AS ttemp SET snapshot_id = ( SELECT MAX(S.snapshot_id) FROM {1}.dss_snapshots S )
;
SELECT DISTINCT
    LOCAL_O.object_id		AS "LocalObjectId", -- 0
    LOCAL_O.object_central_id			AS "Central0bjectId", -- 1
    R.metric_id-1 AS "MetricId", -- 2
-- critical 
CASE	WHEN (
SELECT	MAX(sT.metric_critical)
FROM		{1}.dss_metric_histo_tree sT
WHERE 	sT.metric_id=R.metric_id-1 AND sT.snapshot_id=R.snapshot_id
) = 1	THEN 'Yes'
ELSE		'No'
END AS "Critical", -- 3
QB.metric_id, --4 -- Q.b_criterion_id,
QT.metric_id, --5 -- Q.t_criterion_id,
O.object_name			AS "ObjectName", -- 6
O.object_full_name		AS "ObjectFullname", -- 7
QB.metric_name, QT.metric_name, --8, 9 -- Q.b_criterion_name, Q.t_criterion_name,
Q.metric_name AS "MetricName", -- 10
--
R.snapshot_id AS "SnapshotId", -- 11
O.object_description	AS "ObjectDescription", -- 12
--
R.metric_value_index AS "MetricValueIndex", -- 13
Q.metric_group AS "MetricGroup" --14
FROM
    -- Objects of transaction -> LOCAL_O
    temp_object_ids LOCAL_O
    --
    -- translate local object ids into central object ids
    JOIN {1}.dss_metric_results		R	ON R.object_id = LOCAL_O.object_central_id AND R.snapshot_id+0=LOCAL_O.snapshot_id
    -- for information to be output in select part
    JOIN {1}.dss_metric_types		Q	ON Q.metric_id=R.metric_id-1
	JOIN {1}.dss_metric_type_trees	M2T ON M2T.metric_id=Q.metric_id
	JOIN {1}.dss_metric_types		QT	ON QT.metric_id=M2T.metric_parent_id
	JOIN {1}.dss_metric_type_trees	T2B ON T2B.metric_id=QT.metric_id
	JOIN {1}.dss_metric_types		QB	ON QB.metric_id=T2B.metric_parent_id
    --
    JOIN {1}.dss_objects			O	ON O.object_id=LOCAL_O.object_central_id
	JOIN {1}.dss_metric_histo_tree	HT	ON HT.metric_id=R.metric_id-1
													AND HT.snapshot_id+0=R.snapshot_id
WHERE 1=1
    --AND R.metric_value_index =1 -- 32 sec
	--AND Q.metric_group+0 IN ( 1, 5, 15 ) -- 15 sec
	AND QT.metric_group = 13
	AND QB.metric_group = 10
    AND QB.metric_id = 60017	-- b_criterion_id
    AND 1=HT.metric_critical
"""
queryTransactionObjectsWithCriticalViolations_byX_base="""\
CREATE TEMPORARY TABLE temp_object_ids (
  object_id int4, object_name varchar(1000), object_fullname varchar(1000), object_central_id int4,
  object1_id int4, object1_name varchar(1000), object1_fullname varchar(1000),
  object2_id int4, object2_name varchar(1000), object2_fullname varchar(1000),
  valstr varchar(255), valint int4,
  val1str varchar(255), val1int int4,
  val2str varchar(255), val2int int4,
  snapshot_id int4
)
;
INSERT INTO temp_object_ids
SELECT
    OA.object_id       "ObjectId",				-- -> object_id
    OA.object_name     "ObjectName",			-- -> object_name
    OA.object_fullname	"ObjectFullname",		-- -> object_fullname
    L2C.object_id,										-- -> object_central_id
    --
    OT.object_id		"TransactionId",				-- -> object1_id
    OT.object_name     "TransactionName",				-- -> object1_name
    OT.object_fullname	"TransactionFullname",			-- -> object1_fullname
    --
    NULL, NULL,	NULL,								-- -> object_id, object2_name, object_fullname								
    OT.object_type_str "TransactionType", NULL,		-- -> valstr, valint
    OA.object_type_Str "ArtifactType", NULL,		-- -> val1str, val1int
    'Transaction-object' 	"EntryKind", NULL,			-- -> val2str, val2int
    NULL											-- -> sanpshot_id
FROM
    {0}.cdt_objects				OT
    JOIN {0}.dss_transaction	TR	ON TR.form_id = OT.object_id
    JOIN {0}.dss_objects		OT2	ON OT2.object_id = TR.object_id
    --
    -- --JOIN {0}.dss_links			L ON L.previous_object_id = OT2.object_id
    -- --JOIN {0}.cdt_objects 		OA	ON OA.object_id = L.next_object_id
    JOIN {0}.dss_transactiondetails		DT	ON DT.object_id = OT2.object_id
    JOIN {0}.cdt_objects 		OA	ON OA.object_id = DT.child_id
    --
    JOIN {1}.dss_translation_table	L2C	ON L2C.site_object_id = OA.object_id
WHERE 1=1
    AND OT.object_{3} = {2}
    AND OT2.object_type_id = 30002
;
UPDATE temp_object_ids AS ttemp SET snapshot_id = ( SELECT MAX(S.snapshot_id) FROM {1}.dss_snapshots S )
;
SELECT DISTINCT
    LOCAL_O.object_id		AS "LocalObjectId", -- 0
    LOCAL_O.object_central_id			AS "Central0bjectId", --1
    R.metric_id-1 AS "MetricId", -- 2
-- critical
    'Yes' AS "Critical", --3
QB.metric_id, -- 4 -- Q.b_criterion_id,
QT.metric_id, -- 5 -- Q.t_criterion_id,
O.object_name			AS "ObjectName", -- 6
O.object_full_name		AS "ObjectFullname", -- 7
QB.metric_name, QT.metric_name, -- 8, 9 -- Q.b_criterion_name, Q.t_criterion_name,
Q.metric_name AS "MetricName", -- 10
--
R.snapshot_id AS "SnapshotId", -- 11
O.object_description	AS "ObjectDescription", -- 12
--
R.metric_value_index AS "MetricValueIndex", -- 13
Q.metric_group AS "MetricGroup" -- 14
FROM
    -- Objects of transaction -> LOCAL_O
	    temp_object_ids LOCAL_O
    JOIN {1}.dss_metric_results		R	ON R.object_id = LOCAL_O.object_central_id AND R.snapshot_id=LOCAL_O.snapshot_id
    -- for information to be output in select part
    JOIN {1}.dss_metric_types		Q	ON Q.metric_id=R.metric_id-1
	JOIN {1}.dss_metric_type_trees	M2T ON M2T.metric_id=Q.metric_id
	JOIN {1}.dss_metric_types		QT	ON QT.metric_id=M2T.metric_parent_id
	JOIN {1}.dss_metric_type_trees	T2B ON T2B.metric_id=QT.metric_id
	JOIN {1}.dss_metric_types		QB	ON QB.metric_id=T2B.metric_parent_id
--x---JOIN newtron_cms_central.csv_quality_tree		Q	ON Q.metric_id=R.metric_id-1
    JOIN {1}.dss_objects			O	ON O.object_id=LOCAL_O.object_central_id
	JOIN {1}.dss_metric_histo_tree	HT	ON HT.metric_id=R.metric_id-1
													AND HT.snapshot_id=R.snapshot_id
WHERE 1=1
    --AND R.metric_value_index IN ( 1, 1 ) -- 32 sec
	--AND Q.metric_group+0 IN ( 1, 5, 15 ) -- 15 sec
	AND QT.metric_group = 13
	AND QB.metric_group = 10
    AND QB.metric_id = 60017	-- b_criterion_id
    AND 1=HT.metric_critical
"""

# Pity: filter required in py cause adding where/jon clause breaks the performance
def filterTransactionObjectsWithCriticalViolation( aRow ):
    retVal=True # by default keep the row
    if int(aRow[13])!=1 or int(aRow[14]) not in (1, 5, 15):
        retVal = False
    return retVal


configTransactionObjectsWithCriticalViolations=TQueryConfig(
	countH	= "#NbOfTransactionObjectsWithVC",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryTransactionObjectsWithCV_byX_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#LocalObjectId|CentralObjectId|MetricId|isCritical|"\
    	"BCriterionId|TCriterionId|ObjectName|ObjectFullname|BCriterionName|TCriterionName|MetricName|"\
    	"SnapshotId|ObjectDesc",
	selectQ	= queryTransactionObjectsWithCriticalViolations_byX_base
)



##== Transaction objects with all violation details -------------------------
# {0}: local schema name, {1}: central schema, {2}: transaction name/transaction fullname
# Result set: { "LocalObject", "TransactionName", "RootId", "RootName", "RootFullname", "RootType" }
queryTransactionObjectsWithV_byX_base="""\
--
-- TQI: 60017, Robustness: 60013, Efficiency: 60014, Security: 60016, Changeability: 60012, Transferability: 60011
-- Others: Architectural Design: 66032, Documentation: 66033, Programming Practices: 66031,
--  SEI Maintainability: 60015
-- 
SELECT
	LOCAL_O.object_id AS "ObjectId(Local)", LOCAL_O.object_name AS "ObjectName", O.object_id AS "ObjectId(Central)",
	Q.metric_id AS "MetricId", Q.metric_name AS "MetricName", 
	Q.b_criterion_id AS "BCriterionId", Q.b_criterion_name AS "BCriterionName",
	Q.t_criterion_id AS "TCriterionId", Q.t_criterion_name AS "TCriterionName",
	Q.t_weight AS "TWeight", Q.m_weight AS "MWeight", Q.t_crit AS "TCrit", Q.m_crit AS "MCrit",
	LOCAL_O.transaction_id AS "TransactionId", LOCAL_O.transaction_name AS "TransactionName",
	LOCAL_O.object_fullname AS "ObjectFullname", LOCAL_O.transaction_fullname AS "TransactionFullname"
FROM
	(
		SELECT
		    iO.object_id AS "object_id", iO.object_name AS "object_name",
		    iO.object_fullname AS "object_fullname", iO.object_type_str AS "object_type_str",
		    iOT.object_id AS "transaction_id", iOT.object_name AS "transaction_name",
		    iOT.object_fullname AS "transaction_fullname", iOT.object_type_str AS "transaction_type_str"	
		FROM
			{0}.cdt_objects			iOT
			JOIN {0}.dss_transaction	iT	ON iT.form_id = iOT.object_id
			JOIN {0}.dss_objects		iOT2	ON iOT2.object_id = iT.object_id
			JOIN {0}.dss_links		iTL	ON iTL.previous_object_id = iOT2.object_id
			JOIN {0}.cdt_objects		iO	ON iO.object_id = iTL.next_object_id
		WHERE 1=1
    		AND iOT.object_{3} = {2}
			AND iOT2.object_type_id = 30002
	) AS LOCAL_O
	-- translate local object ids into central object ids
	JOIN {1}.dss_translation_table		L2C	ON L2C.site_object_id = LOCAL_O.object_id
	JOIN {1}.dss_metric_results		R	ON R.object_id = L2C.object_id
	-- for information to be output in select part
	JOIN {1}.dss_objects				O	ON O.object_id=L2C.object_id
	JOIN {1}.csv_quality_tree			Q	ON Q.metric_id=R.metric_id-1
WHERE 1=1
	AND R.metric_value_index=1
	AND Q.b_criterion_id IN ( 60017, 60013, 60014, 60016, 60012, 60011 )
ORDER BY
	LOCAL_O.transaction_id, LOCAL_O.object_id, Q.metric_id, Q.b_criterion_name, Q.t_criterion_name  
"""

configTransactionObjectsWithV=TQueryConfig(
	countH	= "#NbOfTransactionViolations",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryTransactionObjectsWithV_byX_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#0/A:LocalObjectId|1/B:ObjectName|2/C:CentralObjectId|"\
	"3/D:MetricId|4/E:MetricName|"\
    "5/F:BCriterionId|6/G:BCriterionName|7/H:TCriterionId|8/I:TCriterionName|"\
	"9/J:TWeight|10/K:MWeight|11/L:TCrit|12/M:MCrit|"\
	"13/N:TransactionId|14/O:TransactionName|"\
	"15/P:ObjectFullname|16/Q:TransactionFullname",
	selectQ	= queryTransactionObjectsWithV_byX_base
)


## Version with temporary table
# {0}: local schema name, {1}: central schema, {2}: transaction name/transaction fullname
# Result set: { "LocalObject", "TransactionName", "RootId", "RootName", "RootFullname", "RootType" }
queryTransactionObjectsWithViolations_byX_base_DEPRECATED="""\
CREATE TEMPORARY TABLE temp_object_ids2 (
    object_id int4, object_name varchar(1000), object_fullname varchar(1000),
    object_central_id int4,
    transaction_id int4, transaction_name varchar(1000), transaction_fullname varchar(1000),
    transaction_type varchar(255),
    object_type_str varchar(255),
    entry_kind varchar(255),
	snapshot_id int4
)
;
INSERT INTO temp_object_ids2
SELECT
    OA.object_id       "ObjectId",				-- -> object_id
    OA.object_name     "ObjectName",			-- -> object_name
    OA.object_fullname	"ObjectFullname",		-- -> object_fullname
	L2C.object_id,										-- -> object_central_id
	--
	OT.object_id        "TransactionId",				-- -> transaction_id
    OT.object_name      "TransactionName",				-- -> transaction_name
    OT.object_fullname  "TransactionFullname",			-- -> transaction_fullname
	--					
    OT.object_type_str  "TransactionType",		-- -> transaction_type
    OA.object_type_Str  "ArtifactType",			-- -> object_type_str
    'Transaction-object' 	"EntryKind",		-- -> entry_kind
	NULL											-- -> sanpshot_id
FROM
    {0}.cdt_objects				OT
    JOIN {0}.dss_transaction	TR	ON TR.form_id = OT.object_id
	JOIN {0}.dss_objects		OT2	ON OT2.object_id = TR.object_id
	JOIN {0}.dss_links			L ON L.previous_object_id = OT2.object_id
    JOIN {0}.cdt_objects 		OA	ON OA.object_id = L.next_object_id
	--
	JOIN {1}.dss_translation_table	L2C	ON L2C.site_object_id = OA.object_id
WHERE 1=1
	AND OT.object_{3} = {2}
    AND OT2.object_type_id = 30002
;
UPDATE temp_object_ids2 AS ttemp SET snapshot_id = ( SELECT MAX(S.snapshot_id) FROM {1}.dss_snapshots S )
;
SELECT
	LOCAL_O.object_id AS "ObjectId(Local)", LOCAL_O.object_name AS "ObjectName",
	O.object_id AS "ObjectId(Central)",
	Q.metric_id AS "MetricId", Q.metric_name AS "MetricName", 
	Q.b_criterion_id AS "BCriterionId", Q.b_criterion_name AS "BCriterionName",
	Q.t_criterion_id AS "TCriterionId", Q.t_criterion_name AS "TCriterionName",
	Q.t_weight AS "TWeight", Q.m_weight AS "MWeight", Q.t_crit AS "TCrit", Q.m_crit AS "MCrit",
	LOCAL_O.transaction_id AS "TransactionId", LOCAL_O.transaction_name AS "TransactionName",
	LOCAL_O.object_fullname AS "ObjectFullname", LOCAL_O.transaction_fullname AS "TransactionFullname",
	R.snapshot_id AS "SnapshotId"
FROM
	temp_object_ids2             LOCAL_O
	JOIN {1}.dss_metric_results R   ON R.object_id = LOCAL_O.object_central_id and R.snapshot_id=LOCAL_O.snapshot_id
	JOIN {1}.dss_objects        O   ON O.object_id=LOCAL_O.object_central_id
	JOIN {1}.csv_quality_tree   Q   ON Q.metric_id=R.metric_id-1
WHERE 1=1
	AND R.metric_value_index=1
	AND Q.b_criterion_id IN ( 60017, 60013, 60014, 60016, 60012, 60011 )
ORDER BY
	LOCAL_O.transaction_id, LOCAL_O.object_id, Q.metric_name, Q.b_criterion_name, Q.t_criterion_name  
"""

queryTransactionObjectsWithViolations_byX_base="""\
CREATE TEMPORARY TABLE temp_object_ids2 (
    object_id int4, object_name varchar(1000), object_fullname varchar(1000),
    object_central_id int4,
    transaction_id int4, transaction_name varchar(1000), transaction_fullname varchar(1000),
    transaction_type varchar(255),
    object_type_str varchar(255),
    entry_kind varchar(255),
	snapshot_id int4
)
;
INSERT INTO temp_object_ids2
SELECT
    OA.object_id       "ObjectId",				-- -> object_id
    OA.object_name     "ObjectName",			-- -> object_name
    OA.object_fullname	"ObjectFullname",		-- -> object_fullname
	L2C.object_id,										-- -> object_central_id
	--
	OT.object_id        "TransactionId",				-- -> transaction_id
    OT.object_name      "TransactionName",				-- -> transaction_name
    OT.object_fullname  "TransactionFullname",			-- -> transaction_fullname
	--					
    OT.object_type_str  "TransactionType",		-- -> transaction_type
    OA.object_type_Str  "ArtifactType",			-- -> object_type_str
    'Transaction-object' 	"EntryKind",		-- -> entry_kind
	NULL											-- -> sanpshot_id
FROM
    {0}.cdt_objects				OT
    JOIN {0}.dss_transaction	TR	ON TR.form_id = OT.object_id
	JOIN {0}.dss_objects		OT2	ON OT2.object_id = TR.object_id
	-- --JOIN {0}.dss_links			L ON L.previous_object_id = OT2.object_id
    -- --JOIN {0}.cdt_objects 		OA	ON OA.object_id = L.next_object_id
	JOIN {0}.dss_transactiondetails		DT	ON DT.object_id = OT2.object_id
	JOIN {0}.cdt_objects		OA	ON OA.object_id = DT.child_id
	--
	JOIN {1}.dss_translation_table	L2C	ON L2C.site_object_id = OA.object_id
WHERE 1=1
	AND OT.object_{3} = {2}
    AND OT2.object_type_id = 30002
;
UPDATE temp_object_ids2 AS ttemp SET snapshot_id = ( SELECT MAX(S.snapshot_id) FROM {1}.dss_snapshots S )
;
SELECT
	LOCAL_O.object_id AS "ObjectId(Local)", LOCAL_O.object_name AS "ObjectName",
	O.object_id AS "ObjectId(Central)",
	Q.metric_id AS "MetricId", Q.metric_name AS "MetricName", 
	QB.metric_id AS "BCriterionId", QB.metric_name AS "BCriterionName",
	QT.metric_id AS "TCriterionId", QT.metric_name AS "TCriterionName",
	M2T.aggregate_weight AS "TWeight", T2B.aggregate_weight AS "MWeight",
	T2B.metric_critical AS "TCrit", M2T.metric_critical AS "MCrit",
	LOCAL_O.transaction_id AS "TransactionId", LOCAL_O.transaction_name AS "TransactionName",
	LOCAL_O.object_fullname AS "ObjectFullname", LOCAL_O.transaction_fullname AS "TransactionFullname",
	R.snapshot_id AS "SnapshotId"
FROM
	temp_object_ids2             LOCAL_O
    JOIN {1}.dss_metric_results		R	ON R.object_id = LOCAL_O.object_central_id
													AND R.snapshot_id+0=LOCAL_O.snapshot_id
	JOIN {1}.dss_objects        O   ON O.object_id=LOCAL_O.object_central_id
    JOIN {1}.dss_metric_types		Q	ON Q.metric_id=R.metric_id-1
	JOIN {1}.dss_metric_type_trees	M2T ON M2T.metric_id=Q.metric_id
	JOIN {1}.dss_metric_types		QT	ON QT.metric_id=M2T.metric_parent_id
	JOIN {1}.dss_metric_type_trees	T2B ON T2B.metric_id=QT.metric_id
	JOIN {1}.dss_metric_types		QB	ON QB.metric_id=T2B.metric_parent_id
WHERE 1=1
	AND R.metric_value_index=1
	AND QB.metric_id IN ( 60017, 60013, 60014, 60016, 60012, 60011 )
ORDER BY
	LOCAL_O.transaction_id, LOCAL_O.object_id, Q.metric_name, QB.metric_id, QT.metric_id"""

configTransactionObjectsWithViolations=TQueryConfig(
	countH	= "#NbOfTransactionViolations",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryTransactionObjectsWithViolations_byX_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#0/A:LocalObjectId|1/B:ObjectName|2/C:CentralObjectId|"\
	"3/D:MetricId|4/E:MetricName|"\
    "5/F:BCriterionId|6/G:BCriterionName|7/H:TCriterionId|8/I:TCriterionName|"\
	"9/J:TWeight|10/K:MWeight|11/L:TCrit|12/M:MCrit|"\
	"13/N:TransactionId|14/O:TransactionName|"\
	"15/P:ObjectFullname|16/Q:TransactionFullname",
	selectQ	= queryTransactionObjectsWithViolations_byX_base
)




##== Objpro for enlihten view -----------------------------------------------
queryTransactionObjpro_byX_base_DEPRECATED="""\
SELECT DISTINCT
	OPRO.idpro from {0}.objpro	OPRO
WHERE
	OPRO.idobj IN (
		-- transaction objects
		SELECT -- DISTINCT
			O.object_id
		FROM
			{0}.dss_transaction	T
			JOIN {0}.cdt_objects	OT	ON OT.object_id = T.form_id
			JOIN {0}.dss_objects	OT2	ON OT2.object_id = T.object_id
			JOIN {0}.dss_links	L	ON L.previous_object_id = OT2.object_id
			JOIN {0}.cdt_objects	O	on O.object_id = L.next_object_id
		WHERE
			OT.object_{2} = {1}	-- <transaction id>
			AND OT2.object_type_id = 30002
	)
"""

queryTransactionObjpro_byX_base="""\
SELECT DISTINCT
	OPRO.idpro from {0}.objpro	OPRO
WHERE
	OPRO.idobj IN (
		-- transaction objects
		SELECT -- DISTINCT
			O.object_id
		FROM
			{0}.dss_transaction	T
			JOIN {0}.cdt_objects	OT	ON OT.object_id = T.form_id
			JOIN {0}.dss_objects	OT2	ON OT2.object_id = T.object_id
			--JOIN {0}.dss_links	L	ON L.previous_object_id = OT2.object_id
	        JOIN {0}.dss_transactiondetails		DT	ON DT.object_id = OT2.object_id
			JOIN {0}.cdt_objects	O	on O.object_id = DT.child_id
		WHERE
			OT.object_{2} = {1}	-- <transaction id>
			AND OT2.object_type_id = 30002
	)
"""
configTransactionObjPro=TQueryConfig(
	countH	= "#NbOfObjproId",
	countQ	= """SELECT COUNT(0) FROM (
-- -- -- begin -- -- --
--
"""+queryTransactionObjpro_byX_base+"""--
-- -- -- end -- -- --
) AS InnerQuery
""",
	selectH	= "#ObjproId",
	selectQ	= queryTransactionObjpro_byX_base
)



##== ------------------------------------------------------------------------
# aConn : connexion
# aQuery: query that returns a count
# aArgs: arguments to use
def executeQueryCount( aConn, aQuery, aIsDDL, aTraceQuery, *aArgs):
    vFullQuery = aQuery.format( *aArgs )
    if aTraceQuery:
        print( "[DEBUG]  -> query: {\n"+vFullQuery+"}", file=sys.stderr )
    vRs = postgresExecuteQuery( aConn, vFullQuery, aIsDDL )
    vCount = 0+vRs[0][0]
    return vCount

# Returns a tuple: ( 0: count, 1: query effectively executed )
def executeQueryCount2( aConn, aQuery, aIsDDL, aTraceQuery, *aArgs):
    vFullQuery = aQuery.format( *aArgs )
    if aTraceQuery:
        print( "[DEBUG]  -> query: {\n"+vFullQuery+"}", file=sys.stderr )
    vRs = postgresExecuteQuery( aConn, vFullQuery, aIsDDL )
    vCount = 0+vRs[0][0]
    return ( vCount, vFullQuery )


# aConn : connexion
# aQuery: query that returns a count
# aArgs: arguments to use
def executeQueryWithResultSet( aConn, aQuery, aIsDDL, aTraceQuery, *aArgs):
    vFullQuery = aQuery.format( *aArgs )
    if aTraceQuery:
        print( "[DEBUG]  -> query: {"+vFullQuery+"}", file=sys.stderr )
    return postgresExecuteQuery( aConn, vFullQuery, aIsDDL )


# Returns a tuple: ( 0: result set, 1: query effectively executed )
def executeQueryWithResultSet2( aConn, aQuery, aIsDDL, aTraceQuery, *aArgs):
    vFullQuery = aQuery.format( *aArgs )
    if aTraceQuery:
        print( "[DEBUG]  -> query: {"+vFullQuery+"}", file=sys.stderr )
    return ( postgresExecuteQuery( aConn, vFullQuery, aIsDDL ), vFullQuery )



##== ------------------------------------------------------------------------

def createConfigurationFrom( aConfig, aSchemaPrefix, aSchemaPrefixMeasure=None, aSchemaPrefixLocal=None, aSchemaPrefixMng=None, aSchemaPrefixCentral=None ):
    retVal = dict(aConfig)

    retVal['db-prefix'] = aSchemaPrefix
    retVal['db-measure'] = ( retVal['db-prefix'] + "_measure" ) if not aSchemaPrefixMeasure else aSchemaPrefixMeasure
    retVal['db-local'] = ( retVal['db-prefix'] + "_local" ) if not aSchemaPrefixLocal else aSchemaPrefixLocal
    retVal['db-central'] = ( retVal['db-prefix'] + "_central" ) if not aSchemaPrefixCentral else aSchemaPrefixCentral
    retVal['db-mngt'] = ( retVal['db-prefix'] + "_mngt" ) if not aSchemaPrefixMng else aSchemaPrefixMng

    retVal['db-login'] = 'operator'
    retVal['db-password'] = 'CastAIP'
    retVal['db-base'] = 'postgres'

    return retVal


def postgresConnectionString( aDbConfig ):
    #return "host='"+aDbConfig['db-server']+"' port="+aDbConfig['db-port']+" dbname='"+aDbConfig['db-base']+"' user='"+aDbConfig['db-login']+"' password='"+aDbConfig['db-password']+"'"
    return "host='{0}' port={1} dbname='{2}' user='{3}' password='{4}'".format(
            aDbConfig['db-server'], aDbConfig['db-port'], aDbConfig['db-base'], aDbConfig['db-login'], aDbConfig['db-password']
        )

def escapePipe( aStr ):
    return aStr.replace('|','/')

##== The main job here ------------------------------------------------------
def extractTransactionData( aOptions ):

    vWatchTr = common.timewatch.TimeWatch()

    queries = None

    # Do not override
    vDoNotOverrideAll = False   # if transaction has already been dumped => return
    vDoNotOverride = True   # do not override file that has been already created
    vDoNotOverride = not aOptions["override-existing-extract-file"] if "override-existing-extract-file" in aOptions else True

    #if vDoNotOverrideAll and os.path.isfile( os.path.join(aOutputFolderPath,"40_objects-objpro.txt") ) :
    if vDoNotOverrideAll and os.path.isfile( os.path.join(aOutputFolderPath,"40_objects-objpro.txt") ) :
        logger.warning( "!!!WARNING: already computed: skipping.")
        return

    aCriteria = aOptions['transaction-criteria']
    aCriteriaValue = aOptions['transaction-criteria-value']
    aCriteriaOp = aOptions['transaction-criteria-op']
    aWithViolations = aOptions['with-violations']
    aOutputFolderPath = aOptions['tr-output-data-folder']
    aConfig = aOptions['db-config']
    if None != aCriteria:
        if "id" != aCriteria:
            vCriteriaValue = "'{}'".format(aCriteriaValue)
        else:
            vCriteriaValue = aCriteriaValue
            
        # Query map is intanciated with the proper criteria
        queries = [
            ( "List-of-all-transactions", False, configAllTransactions.selectQ, ( aConfig['db-local'],), configAllTransactions.selectH, "00b_all-transactions.txt", False, False, None ),
            ( "List-of-transactions", False, configTransactions.selectQ, ( aConfig['db-local'], vCriteriaValue, aCriteria), configTransactions.selectH, "01b_transactions.txt", False, False, None ),
            ( "List-of-transaction-objects", False, configTransactionObjects.selectQ, ( aConfig['db-local'], vCriteriaValue, aCriteria), configTransactionObjects.selectH, "10b_transaction-objects.txt", False, False, None ),
            ( "List-of-transaction-links", False, configTransactionLinks.selectQ, ( aConfig['db-local'], vCriteriaValue, aCriteria), configTransactionLinks.selectH, "20b_transaction-links.txt", False, False, None ),
        ]
        if aWithViolations:
            queries.extend( [
                ( "List-of-transaction-objects-with-critical-violations", False, configTransactionObjectsWithCriticalViolations.selectQ, ( aConfig['db-local'], aConfig['db-central'], vCriteriaValue, aCriteria), configTransactionObjectsWithCV.selectH, "30_objects-with-violations.txt", False, False, filterTransactionObjectsWithCriticalViolation ),
                ( "List-of-transaction-objects-with-violations", False, configTransactionObjectsWithViolations.selectQ, ( aConfig['db-local'], aConfig['db-central'], vCriteriaValue, aCriteria), configTransactionObjectsWithViolations.selectH, "32_object-violations.txt", False, False, None ),
            ] )
        queries.extend( [
            ( "List-of-transaction-endpoints", False, configTransactionEndpoints.selectQ, ( aConfig['db-local'], vCriteriaValue, aCriteria), configTransactionEndpoints.selectH, "31b_transaction-endpoints.txt", False, False, None ),
            ( "List-of-obj-pro", False, configTransactionObjPro.selectQ, ( aConfig['db-local'], vCriteriaValue, aCriteria), configTransactionObjPro.selectH, "40_objects-objpro.txt", False, False, None ),
        ] )
    else:
        logger.error( "***ERROR: Wrong arguments: criteria='{0}', value='{1}', operator='{2}', skipping.".format(aCriteria,aCriteriaValue,aCriteriaOp) )
        return

    vTraceAllSqlQueries = False
    vTraceSqlQueriesOnly = False # TODO: not yet implemented
    vStopAfter = -1

    vQueryPerfs = []
    vCnxStr = postgresConnectionString(aConfig)
    logger.info( "    using connection string: {}".format(vCnxStr) )
    with psycopg2.connect(vCnxStr) as vConn:

        vWatch = common.timewatch.TimeWatch()
        vQueryNum = 0
        for iQuery in queries:
            logger.info( "    executing query: {0}...".format(iQuery[0]) )
            vWatch.start()
            vQuery = None
            if iQuery[1]:
                # query that returns a count
                vCount, vQuery = executeQueryCount2( vConn, iQuery[2], iQuery[6], vTraceAllSqlQueries or iQuery[7], *iQuery[3] )
                vWatch.stop()
                logger.info( "      -> {0}: {1}".format(iQuery[0], vCount) )
                logger.info( "      elapsed: {}, cpu: {}".format(vWatch.deltaElapsed(), vWatch.deltaCpu()) )
                vQueryPerfs.append( ( iQuery[5], vWatch.deltaElapsed(), vWatch.deltaCpu() ))
            else:
                vFilePath = os.path.join(aOutputFolderPath,iQuery[5])
                if vDoNotOverride and os.path.isfile( vFilePath ) :
                    logger.warning( "!!!WARNING: skipping file [{}]: already generated".format(vFilePath) )
                    continue

                # query that returns a row set
                vRs, vQuery = executeQueryWithResultSet2( vConn, iQuery[2], iQuery[6], vTraceAllSqlQueries or iQuery[7], *iQuery[3] )
                vFile = None
                if iQuery[5]:
                    logger.info( "      writing to [{}]".format(vFilePath) )
                    vFile = open( vFilePath, "w")
                    # write headers
                    if iQuery[4]:
                        print( iQuery[4], file=vFile )

                vRowNum = 0
                logger.info( "      {0}:".format(iQuery[0]) )
                if vRs:
                    vFilter = iQuery[8]
                    for vRow in vRs:
                        if not vFilter or vFilter(vRow):
                            vRowNum += 1
                            vLine = '|'.join( [escapePipe(str(e)) for e in vRow] )
                            if vFile:
                                print( vLine, file=vFile )
                            else:
                                logger.info( "[Row:{:>6}]:{}".format(vRowNum,vLine) )
                                if vRowNum > 10:
                                    break
                if vFile:
                    vFile.close()
                vWatch.stop()
                logger.info( "      elapsed: {}, cpu: {}".format(vWatch.deltaElapsed(), vWatch.deltaCpu()) )
                vQueryPerfs.append( ( iQuery[5], vWatch.deltaElapsed(), vWatch.deltaCpu() ))
            
            if True and None!=vQuery:
                # save query in file
                with open( os.path.join(aOutputFolderPath,iQuery[5])+".sql", "w" ) as vFileQ:
                    print( vQuery, file=vFileQ )

            vQueryNum += 1
            if -1!=vStopAfter and vQueryNum>vStopAfter:
                break 

    vWatchTr.stop()
    #vQueryPerfs.append( ( aTransactionName, vWatchTr.deltaElapsed(), vWatchTr.deltaCpu() ))
    vQueryPerfs.append( ( aOptions['transaction'], vWatchTr.deltaElapsed(), vWatchTr.deltaCpu() ))
    with open( os.path.join(aOutputFolderPath,"query-performances.txt"), "a+") as vF:
        for i in vQueryPerfs:
            print( "[{}] {}: elapsed: {}, cpu: {}".format(common.timewatch.TimeWatch.generateDateTimeMSecStamp(),i[0],i[1],i[2]), file=vF )


def kytExtractMain( aArgv ):

    if 0==len(aArgv) or not os.path.isfile( aArgv[0] ):
        logger.error( "***ERROR: No valid configuration file, exiting." )
        return 1

    vConfiguration = common.config.CConfig(aArgv[0])

    vConfiguration.processConfigurations(extractTransactionData,None,False)
    return 0


if __name__ == "__main__":
    logger.info( "Starting..." )
    vWatch = common.timewatch.TimeWatch()
    kytExtractMain( sys.argv[1:] )
    vWatch.stop()
    logger.info( "Finished: elapsed: {}, cpu: {}".format(vWatch.deltaElapsed(),vWatch.deltaCpu()) )