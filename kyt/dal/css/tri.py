import os
import sys
import psycopg2
import collections

import logging

logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.DEBUG
)

TTransactionTri = collections.namedtuple( "TTransactionTri", ["hf","tri","fullname"])

##== ------------------------------------------------------------------------
def _postgresExecuteQuery( aConn, aQuery, aIsDDL=False ):
    retVal = []
    vCurs = aConn.cursor()
    #logger.info( "query:\n{}\n------".format(aQuery) )
    vRes = vCurs.execute( aQuery )
    #print( "-- begin - query ------\n{0}\n--  end  - query ------".format(aQuery), file=sys.stderr )
    if not aIsDDL:
        retVal = vCurs.fetchall()
    return retVal

# aConn : connexion
# aQuery: query that returns a count
# aArgs: arguments to use
def _executeQueryWithResultSet( aConn, aQuery, aIsDDL, aTraceQuery, *aArgs):
    vFullQuery = aQuery.format( *aArgs )
    if aTraceQuery:
        print( "[DEBUG]  -> query: {"+vFullQuery+"}", file=sys.stderr )
    return _postgresExecuteQuery( aConn, vFullQuery, aIsDDL )


# Returns a tuple: ( 0: result set, 1: query effectively executed )
def _executeQueryWithResultSet2( aConn, aQuery, aIsDDL, aTraceQuery, *aArgs):
    vFullQuery = aQuery.format( *aArgs )
    if aTraceQuery:
        print( "[DEBUG]  -> query: {"+vFullQuery+"}", file=sys.stderr )
    return ( _postgresExecuteQuery( aConn, vFullQuery, aIsDDL ), vFullQuery )





# Return list of transaction with TRI, health factor and transaction fullname
# Result set: 0: health factor ID, 1: TRI value, 2: transaction full name
# {0} : central schema name
C_QUERY_CENTRAL_TRI = """
SELECT TRI.bc_id, TRI.tri, OBJ.object_full_name
FROM
	{0}.dss_tri_transaction TRI
	JOIN {0}.dss_objects OBJ ON OBJ.object_id = TRI.transaction_id
WHERE
	TRI.snapshot_id = ( SELECT MAX(S.snapshot_id) FROM {0}.dss_snapshots S)
ORDER BY
	TRI.bc_id ASC, TRI.tri DESC, OBJ.object_full_name ASC
"""
# {0} : central schema name
# {1} : local schema name
C_QUERY_CENTRAL_TRI2 = """
SELECT TRI.bc_id, TRI.tri, OBJCEN.object_full_name
FROM
	{0}.dss_tri_transaction TRI
	JOIN {0}.dss_objects OBJCEN ON OBJCEN.object_id = TRI.transaction_id
    JOIN {1}.cdt_objects OBJLOC ON OBJLOC.object_fullname = OBJCEN.object_full_name
    JOIN {1}.dss_transaction TRLOC ON TRLOC.form_id=OBJLOC.object_id
WHERE
	TRI.snapshot_id = ( SELECT MAX(S.snapshot_id) FROM {0}.dss_snapshots S)
ORDER BY
	TRI.bc_id ASC, TRI.tri DESC, OBJCEN.object_full_name ASC
"""

C_CAST_HF = { 60013:"ROB", "60013":"ROB", 60014:"EFF", "60014":"EFF", 60016:"SEC", "60016":"SEC" }

def extractTransactionTri( aCssServer, aCssPort, aCssLogin, aCssPassword, aCssDb, aSchemaPrefix, aLimit ):
    retVal = { "ROB":[], "EFF":[], "SEC":[] }
    vCnxStr = "host='{0}' port={1} dbname='{2}' user='{3}' password='{4}'".format(
        aCssServer, aCssPort, aCssDb, aCssLogin, aCssPassword )
    
    with psycopg2.connect(vCnxStr) as vConn:
        #vRs, vQuery = _executeQueryWithResultSet2( vConn, C_QUERY_CENTRAL_TRI, False, False, aSchemaPrefix+"_central" )
        vRs, vQuery = _executeQueryWithResultSet2( vConn, C_QUERY_CENTRAL_TRI2, False, False, aSchemaPrefix+"_central", aSchemaPrefix+"_local" )
        if vRs:
            for iRow in vRs:
                if iRow[0] in C_CAST_HF:
                    vHf = C_CAST_HF[iRow[0]]
                    if len(retVal[vHf]) < aLimit:
                        retVal[vHf].append( TTransactionTri(vHf, iRow[1], iRow[2]) )
                else:
                    print( "***ERROR: unknown hf: {}".format(iRow), file=sys.stderr )
    return retVal
