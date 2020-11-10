import sys
import os
import json
import time
import datetime
import collections
import traceback
import logging
logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.INFO
)

import importlib
from common import timewatch
from common import config 
from dal import n_dal
from common import n_graph
from model import n_data
from render import gviz
#from process.dfsAlgo import algo_longest_path
#from process.dfsAlgo import algo_longest_path2
from process.dfsAlgo import algo_paths_of_interest
from process.dfsAlgo import algo_paths_of_interest2
from process import process_config
from output import output_paths

gFileNames={
    "transaction-objects"       : ( "10b_transaction-objects", "txt" ),
    "transaction-links"         : ( "20b_transaction-links", "txt" ),
    "transaction-endpoints"     : ( "31b_transaction-endpoints", "txt" ),
    "object-critical-violations"    : ( "30_objects-with-violations", "txt" ),
    "object-violations"         : ( "32_object-violations", "txt" ),
    "enlighten-objects"         : ( "99_objects", "txt" ),
    "enlighten-objects2"         : ( "99_objects2", "txt" ),
    "enlighten-objects-all2"     : ( "99_objects-all", "txt" ),
    "enlighten-objects-all"     : ( "99_objects-all2", "txt" ),
}


#DFSContext = collections.namedtuple('DFSContext', [ 'shortestPath','longestPath','criticalestPath'])




## Cast Health factors ------------------------------------------------------
class CCastHealthFactor:
    _MAP_STRING_2_HF=[
        ( 60013, ( "60013", "robustness", "rob", "rob.", "robu", "robu." ) ),
        ( 60014, ( "60014", "efficiency", "eff", "eff.", "performance", "perf.", "perf" ) ),
        ( 60016, ( "60016", "security", "sec", "secu", "sec.", "secu." ) ),
        ( 60012, ( "60012", "changeability", "chng", "chng.", "evol", "evol." ) ),
        ( 60011, ( "60011", "transferability", "trans", "trans." ) ),
    ]
    _mapString2Hf = {}

    def initialiseMap():
        if None == CCastHealthFactor._mapString2Hf:
            for x in CCastHealthFactor._MAP_STRING_2_HF:
                for y in x[1]:
                    CCastHealthFactor._mapString2Hf[y] = x[0]

    def healthFactorId( aHF ):
        CCastHealthFactor.initialiseMap()
        retVal = -1
        vHF = str(aHF).lower()

        if vHF in CCastHealthFactor._mapString2Hf:
            retVal = CCastHealthFactor._mapString2Hf[vHF]
        return retVal

        vHF = str(aHF)
        # Check against Robustness
        if vHF.lower() in ( "60013", "robustness", "rob", "rob.", "robu", "robu." ):
            retVal = 60013

        # Check against Efficiency
        elif vHF.lower() in ( "60014", "efficiency", "eff", "eff.", "performance", "perf.", "perf" ):
            retVal = 60014

        # Check against Security
        elif vHF.lower() in ( "60016", "security", "sec", "secu", "sec.", "secu." ):
            retVal = 60016

        # Check against Changeability
        elif vHF.lower() in ( "60012", "changeability", "chng", "chng.", "evol", "evol." ):
            retVal = 60012

        # Check against Transferability
        elif vHF.lower() in ( "60011", "transferability", "trans", "trans." ):
            retVal = 60011
        
        return retVal

def formatPath( aBaseFilePath, aFile, aIndex=None ):
    logger.debug( "formatPath({},{})".format(aBaseFilePath,aFile) )
    if aIndex:
        return os.path.join( aBaseFilePath, "{}-{}.{}".format(
            gFileNames[aFile][0], aIndex, gFileNames[aFile][1] ) )
    else:
        return os.path.join( aBaseFilePath, "{0}.{1}".format(
            gFileNames[aFile][0], gFileNames[aFile][1] ) )


##== Graph exploration algorithms -------------------------------------------
def algoTemplate( aGraph, aRootNode, aObjectsOfInterest, aLeavesOfInterest ):
    pass


# Algo interface:
# Return a pair Path, AllPath
#   Path: pair ( 0: nb of CV in path, 1: path tuple )
#   AllPaths: list of paths [ pair ( 0: nb of CV in path, 1: path tuple )] 
def callAlgo( aAlgo, aGraph, aRootNode, aObjectsOfInterest, aLeavesOfInterest, aOptions ):
    logger.info( "using algo: {}: {}: {}.{}".format(aAlgo.short_name,aAlgo.name,aAlgo.module,aAlgo.main) )
    vAlgoFn = getattr( importlib.import_module(aAlgo.module), aAlgo.main)
    retRes, retAllPaths = vAlgoFn( aGraph, aRootNode, aObjectsOfInterest, aLeavesOfInterest )

    if None!=retRes and None!=retRes[1]:
        logger.info( "  path found -> {}".format(retRes) )
        logger.info( "    nb of cv : {}".format(retRes[0]) )
        logger.info( "    of length: {}".format(len(retRes[1])) )
    else:
        logger.info( "  no path found" )

    logger.info( "  -> nb paths from root: {}".format(len(retAllPaths[aRootNode._num])) )
    
    if 1 == len(retAllPaths[aRootNode._num]):
        assert(retAllPaths[aRootNode._num][0][1][0]==aRootNode._num)
        logger.info( "  -> nb paths from child1: {}: {}".format(
            retAllPaths[aRootNode._num][0][1][1],
            None if retAllPaths[aRootNode._num][0][1][1] not in retAllPaths else (retAllPaths[retAllPaths[aRootNode._num][0][1][1]])
        ))
        if retAllPaths[aRootNode._num][0][1][1] in retAllPaths and 1 < len(retAllPaths[retAllPaths[aRootNode._num][0][1][1]]):
            vChildNd = aGraph.node(retAllPaths[aRootNode._num][0][1][1])
            retAllPaths[aRootNode._num] = [ x for x in retAllPaths[vChildNd._num] ]
    return retRes, retAllPaths

TAlgoResult = collections.namedtuple( "TAlgoResult", [ "algo", "res1", "resAll"] )
def searchPathsOfInterest( aGraph, aRootNodeNum, aObjectNums, aLeafNums, aOptions=None ):
    retVal = []
    vRootNode = aGraph.node(aRootNodeNum)
    vAllPaths = None
    vRes = None
 
    logger.info( "-- Computing paths..." )
    vAlgorithms = process_config.C_PROCESS_CONFIG["algorithms"]
    for iAlgo in vAlgorithms:
        logger.info( "-- ------------------------------------------------------------------------" )
        vRes, vAllPaths = callAlgo( iAlgo, aGraph, vRootNode, aObjectNums, aLeafNums, aOptions )
        #retVal.append( ( vRes, vAllPaths ) )
        retVal.append( TAlgoResult( iAlgo, vRes, vAllPaths ) )

    logger.info( "-- ------------------------------------------------------------------------" )
    
    logger.info( "-- ------------------------------------------------------------------------" )
    # Experimental algorithms to be put in process_config
    if True:
        for iAlgo in (
            # 0: algo decl, 1: algo routine, 2: first arg to routine, 3: second arg, 4: kind of results
            ( process_config.TAlgoDecl("Paths of interest, no endpoints", "10", None, None), algo_paths_of_interest.pathsToObjectsOfInterest, set(), False, True ),
            ( process_config.TAlgoDecl("Paths of interest, relevant endpoints", "11", None, None), algo_paths_of_interest.pathsToObjectsOfInterest, aLeafNums, False, True ),
            ( process_config.TAlgoDecl("Paths of interest, all endpoints", "12", None, None), algo_paths_of_interest.pathsToObjectsOfInterest, aLeafNums, True, True ),
            ( process_config.TAlgoDecl("Paths of interest, all endpoints, all in one", "13", None, None), algo_paths_of_interest2.pathsToObjectsOfInterest2, aLeafNums, True, False )
        ):
            logger.info( "using algo {}: {}...".format(iAlgo[0].short_name,iAlgo[0].name) )
            vRes = iAlgo[1]( aGraph, vRootNode, aObjectNums, iAlgo[2], iAlgo[3] )
            logger.info( "  -> nb paths: {}".format(len(vRes)) )
            if iAlgo[4]:
                for n,i in enumerate(vRes): logger.debug( "    {:<3}: {}: {}: {}:{}".format(n,i[0],i[1],len(i[2]), i[2]) )
                vAllPaths = { vRootNode._num : [ (len(x[2]), x[2]) for x in vRes ] }
            else:
                for n,i in enumerate(vRes): logger.debug( "    {:<3}: {}: {}".format(n,len(i), i) )
                vAllPaths = { vRootNode._num : [ (len(x), x) for x in vRes ] }
            #retVal.append( ( None, vAllPaths ) )
            retVal.append( TAlgoResult( iAlgo[0], None, vAllPaths ) )
            #print( "{}: {}".format(len(vAllPaths[vRootNode._num]),vAllPaths[vRootNode._num] ) )
    else:
        logger.info( "-- ------------------------------------------------------------------------" )
        iAlgo = ( process_config.TAlgoDecl("Paths of interest, no endpoints", "10", None, None), algo_paths_of_interest.pathsToObjectsOfInterest, set(), False, True )
        logger.info( "using algo {}: {}...".format(iAlgo[0].short_name,iAlgo[0].name) )
        vRes = iAlgo[1]( aGraph, vRootNode, aObjectNums, iAlgo[2], iAlgo[3] )
        logger.info( "  -> nb paths: {}".format(len(vRes)) )
        for n,i in enumerate(vRes):
            logger.debug( "    {:<3}: {}: {}: {}:{}".format(n,i[0],i[1],len(i[2]), i[2]) )
        vAllPaths = { vRootNode._num : [ (len(x[2]), x[2]) for x in vRes ] }
        retVal.append( TAlgoResult( iAlgo[0], None, vAllPaths ) )
        #print( "{}: {}".format(len(vAllPaths[vRootNode._num]),vAllPaths[vRootNode._num] ) )

    """    
    logger.info( "-- ------------------------------------------------------------------------" )
    vAlgoDecl = TAlgoDecl("Paths of interest, no endpoints", "10", None, None)
    logger.info( "Using algo (B): paths of interest, relevant endpoints..." )
    vRes = algo_paths_of_interest.pathsToObjectsOfInterest( aGraph, vRootNode, aObjectNums, aLeafNums, False )
    logger.info( "  -> nb paths: {}".format(len(vRes)) )
    for n,i in enumerate(vRes):
        logger.debug( "    {:<3}: {}: {}: {}:{}".format(n,i[0],i[1],len(i[2]), i[2]) )
    vAllPaths = { vRootNode._num : [ (len(x[2]), x[2]) for x in vRes ] }
    retVal.append( ( None, vAllPaths ) )
    #print( "{}: {}".format(len(vAllPaths[vRootNode._num]),vAllPaths[vRootNode._num] ) )
    
    
    logger.info( "-- ------------------------------------------------------------------------" )
    logger.info( "Using algo (C): paths of interest, all endpoints..." )
    vRes = algo_paths_of_interest.pathsToObjectsOfInterest( aGraph, vRootNode, aObjectNums, aLeafNums, True )
    logger.info( "  -> nb paths: {}".format(len(vRes)) )
    for n,i in enumerate(vRes):
        logger.debug( "    {:<3}: {}: {}: {}:{}".format(n,i[0],i[1],len(i[2]), i[2]) )
    vAllPaths = { vRootNode._num : [ (len(x[2]), x[2]) for x in vRes ] }
    retVal.append( ( None, vAllPaths ) )
    #print( "{}: {}".format(len(vAllPaths[vRootNode._num]),vAllPaths[vRootNode._num] ) )
    
    logger.info( "-- ------------------------------------------------------------------------" )
    logger.info( "Using algo (D): paths of interest, all endpoints..." )
    vRes = algo_paths_of_interest2.pathsToObjectsOfInterest2( aGraph, vRootNode, aObjectNums, aLeafNums, True )
    logger.info( "  -> nb paths: {}".format(len(vRes)) )
    for n,i in enumerate(vRes):
        logger.debug( "    {:<3}: {}: {}".format(n,len(i), i) )
    vAllPaths = { vRootNode._num : [ (len(x), x) for x in vRes ] }
    retVal.append( ( None, vAllPaths ) )
    #print( "{}: {}".format(len(vAllPaths[vRootNode._num]),vAllPaths[vRootNode._num] ) )
    """    
    
    logger.info( "== --------------------------------------------------------------" )
    logger.info( "" )

    return retVal    



def filterAgainstHF( aHealthFactors, aViolation ):
    return ( None == aHealthFactors ) or ( 0 == len(aHealthFactors) ) or ( int(aViolation._b_criterion) in aHealthFactors )




def loadFacts( aBaseFilePath, aOptions ):
    retVal = { "_objects":None, "_graph":None, "_endPoints":None, "_objectsWithCV":None, "_objectsWithV":None }

    vTrObjectsFilePath = formatPath( aBaseFilePath, 'transaction-objects' )
    logger.info( "Loading transaction object from [{}]...".format(vTrObjectsFilePath) )
    retVal["_objects"] = n_dal.loadObjects( vTrObjectsFilePath )

    vTrGraphFilePath = formatPath( aBaseFilePath, 'transaction-links' )
    logger.info( "Loading transaction links from [{}]...".format(vTrGraphFilePath) )
    vGraph = n_dal.loadGraph( vTrGraphFilePath, retVal["_objects"] )
    retVal["_graph"] = vGraph

    vTrEndPointsFilePath = formatPath( aBaseFilePath, 'transaction-endpoints' )
    if os.path.isfile( vTrEndPointsFilePath ):
        logger.info( "Loading end points from [{}]...".format(vTrEndPointsFilePath) )
        retVal["_endPoints"] = n_dal.loadTransactionEndpoints( vTrEndPointsFilePath )
    else:
        logger.info( "No end points file [{}]...".format(vTrEndPointsFilePath) )
        retVal["_endPoints"] = []


    vTrObjectsWCvFilePath = formatPath( aBaseFilePath, 'object-critical-violations' )
    logger.info( "Loading objects with critical violations from [{}]...".format(vTrObjectsWCvFilePath) )
    vObjectsWithVC = {}
    vObjectsNums = None
    if "with-critical-violations" in aOptions and aOptions["with-critical-violations"]:
        vObjectsNum = n_dal.loadObjectsWithViolation( vTrObjectsWCvFilePath, vGraph, vObjectsWithVC )
        logger.info( "  nb objects: {}, nb object with critical violations: {}".format(len(vObjectsNum),len(vObjectsWithVC)) )
    else:
        logger.info( ">>>>>> not loading objects with critical violation." )
    retVal["_objectsWithCVNum"] = vObjectsNum
    retVal["_objectsWithCV"] = vObjectsWithVC

    vTrObjectsWVFilePath = formatPath( aBaseFilePath, 'object-violations' )
    if os.path.isfile( vTrObjectsWVFilePath ):
        logger.info( "Loading objects with violations from [{}]...".format(vTrObjectsWVFilePath) )
        vObjectsWithV = {}
        n_dal.loadObjectsWithViolations( vTrObjectsWVFilePath, vGraph, vObjectsWithV )
        logger.info( "  nb object with violations: {}".format(len(vObjectsWithV.keys())) )
        retVal["_objectsWithV"] = vObjectsWithV
    else:
        logger.info( "No violations file [{}]...".format(vTrObjectsWVFilePath) )
        retVal["_objectsWithV"] = {}

    return retVal


def getTransactionInfo( aBaseFilePath ):
    retValTrId = "<unknown>"
    retValTrName = "<unknown>"
    with open( formatPath( aBaseFilePath, 'transaction-objects' ) ) as vFile:
        for vLine in vFile:
            if '#' != vLine[0]:
                vFields = vLine.strip().split('|')
                retValTrId = int(vFields[0])
                retValTrName = vFields[1]
                break

    return retValTrId, retValTrName

def infereRootNode( aGraph, aTransactionId, aRootNodeId, aTableObjects, aEndPoints ):
    vRootNode = aGraph.nodeFromObj( aRootNodeId ) if aRootNodeId else None
    if None != vRootNode:
        retVal = vRootNode
    else:
        vNodesWithoutCallers = aGraph.nodesWithNoCaller()
        logger.info( "Objects without callers: {}: {}".format(len(vNodesWithoutCallers),vNodesWithoutCallers) )
        
        vMaxAccessibles = 0 
        vRealRootNode = vRootNode
        for iNum in vNodesWithoutCallers:
            vNode = aGraph.node(iNum)
            vRes5b = aGraph.accessibleNodes( vNode, aTableObjects, aEndPoints )
            logger.info( "-> Accessible nodes: {0:>4} out of {1:>4}: tables: {3:>3}, endpoints: {4:>3}: {2}".format(
                len(vRes5b[0]),len(aGraph._nodes),vNode.formatNode(),vRes5b[2],vRes5b[3]) )
            if len(vRes5b[0]) > vMaxAccessibles:
                vRealRootNode = vNode
                vMaxAccessibles = len(vRes5b[0])

        if None != vRealRootNode:
            logger.info( ">>>>>> Using node {0} as root node instead of {1}".format(vRealRootNode.formatNode(),vRootNode.formatNode() if vRootNode else None) )
            retVal = vRealRootNode
        else:
            retVal = aGraph.nodeFromObj( aTransactionId )
            if None == retVal:
                logger.warning( "!!!WARNING: Could not find any potential root node ! Skipping." )
            else:
                logger.info( ">>>>>> Using node {0} as root".format(retVal.formatNode()) )
    return retVal

def computeTransactionPath( aOptions ):
    vOptions2 = aOptions
    vBaseFilePath = aOptions['tr-output-folder']
    vRootNodeId = aOptions['transaction']['root-object-id'] if 'root-object-id' in aOptions['transaction'] else None
    vOptions = aOptions
    vWithViolationObjects = os.path.exists(os.path.join(aOptions['tr-output-data-folder'],"30_objects-with-violations.txt"))
    computePathes( vOptions2, vBaseFilePath, vRootNodeId, aOptions, vWithViolationObjects )

def computePathes( aOptions2, aBaseFilePath, aRootNodeId, aOptions, aWithViolationObjects=False ):
    logger.info( "-- Processing transaction [{0}]".format(aBaseFilePath) )
    
    vTrOutputFolder = aOptions['tr-output-folder']
    vTrOutputDataFolder = aOptions['tr-output-data-folder']
    vTransactionId, vTransactionName = getTransactionInfo(vTrOutputDataFolder)

    vFacts = loadFacts( vTrOutputDataFolder, aOptions )
    vGraph = vFacts["_graph"]

    vEndpointNums = { vGraph.numFromObj(x) for x in vFacts["_endPoints"] }
    vTableObjects = { x._num for x in vGraph._nodes if ( x._obj._object_type=="Oracle table" or x._obj._object_type=="Table" ) }
    logger.info( "  nb of transaction's endpoints: {}".format(len(vEndpointNums)) )
    logger.info( "  nb of transaction's tables: {}".format(len(vTableObjects)) )
    
    # merge table and endpoints
    for x in vTableObjects:
        vEndpointNums.add( x )

    vObjectsWithVC = vFacts["_objectsWithCV"]
    vObjectsNum = vFacts["_objectsWithCVNum"]
    vObjectsWithV = vFacts["_objectsWithV"]

    vGraph.dumpGraph( os.path.join(vTrOutputFolder,"z_dump_gnodes.txt"), os.path.join(vTrOutputFolder,"z_dump_graph.txt") )

    # Get healtfactors to filter against
    vHealthFactors = set()
    if "health-factors" in aOptions:
        for iHF in aOptions["health-factors"]:
            vHF = CCastHealthFactor.healthFactorId(iHF)
            if -1 != vHF:
                vHealthFactors.add( vHF )
                logger.info( "adding health factor [{}] to violation filter".format(vHF) )
    
    # Use filter against list of objects with critical violations
    vObjectsWithVCFiltered = {}
    vNbFilteredObjects = 0
    vNbObjectsToFilter = 0
    for iOwV, iOV in vObjectsWithVC.items():
        vV = []
        for iV in iOV:
            vNbObjectsToFilter += 1
            if filterAgainstHF( vHealthFactors, iV ):
                vV.append( iV)
        if len(vV) > 0:
            vObjectsWithVCFiltered[iOwV] = vV
    logger.info( "Nb objects with vc after filtering: {} out of {}".format(len(vObjectsWithVCFiltered),vNbObjectsToFilter) )

    # Use filter against list of objects with violations
    # and objects with critical violations
    vObjectsWithVFiltered = {}
    vNbFilteredObjects = 0
    vNbObjectsToFilter = 0
    for iOwV, iOV in vObjectsWithV.items():
        vV = []
        for iV in iOV:
            vNbObjectsToFilter += 1
            if filterAgainstHF( vHealthFactors, iV ):
                vV.append( iV)
        if len(vV) > 0:
            vObjectsWithVFiltered[iOwV] = vV
    logger.info( "Nb objects with v after filtering: {}".format(len(vObjectsWithVFiltered),vNbObjectsToFilter) )
    vObjectsWithV = vObjectsWithVFiltered

    # Retrieve root node
    vRootNode = infereRootNode( vGraph, vTransactionId, aRootNodeId, vTableObjects, vEndpointNums )
    if None == vRootNode:
        logger.warning( "!!!WARNING: Could not find any potential root node ! Skipping." )
        return

    logger.info( "-- Root object:\n  ".format(vRootNode.formatNode()) )
    vRes5 = vGraph.accessibleNodes( vRootNode, vTableObjects, vEndpointNums )
    logger.info( "  -> Accessibles nodes: {0} out of {1}: {2}".format(len(vRes5[0]),len(vGraph._nodes),vRootNode.formatNode()) )
    logger.info( "  ->   accessibles tables: {0}".format(vRes5[2]) )
    logger.info( "  ->   accessibles endpoints: {0}".format(vRes5[3]) )
    vNum = 0
    for iNum in vRes5[1]:
        vNum += 1
        vNode = vGraph.node(iNum)
        logger.info( "  object_id: {:>8}, name: {}, fullpath: {}".format(vNode._obj._object_id,vNode._obj._object_name,vNode._obj._object_fullname ) )
        if vNum>6:
            break

    vAlgoResults = searchPathsOfInterest(
        vGraph, vRootNode._num, vObjectsNum, vEndpointNums
    )

    # Output results
    vCastTransaction = n_data.CastTransaction(vTransactionId,vTransactionName,vRootNode,vObjectsWithV,vEndpointNums)
    for iN, iAlgoRes in enumerate(vAlgoResults):
        # Output best path found if any
        #vRes = iAlgoRes[0]
        vRes = iAlgoRes.res1
        logger.info( "-- ---------------------------------------------" )
        logger.info( "outputing results of algo {}...".format(iAlgoRes.algo.short_name) )
        

        # Output path in path and gviz formats
        vOPathFilePath = formatPath( os.path.join(aBaseFilePath,"_paths"), 'enlighten-objects', iAlgoRes.algo.short_name )
        vOGvizFilePath = formatPath( os.path.join(aBaseFilePath,"_gviz"), 'enlighten-objects', iAlgoRes.algo.short_name )+".gviz"
        output_paths.outputPaths( aOptions2, vOPathFilePath, vOGvizFilePath,
            vGraph, vCastTransaction, { vRootNode._num : [vRes] } )

        # Output all paths
        vOPathFilePath = formatPath( os.path.join(aBaseFilePath,"_paths"), 'enlighten-objects-all2', iAlgoRes.algo.short_name )
        vOGvizFilePath = formatPath( os.path.join(aBaseFilePath,"_gviz"), 'enlighten-objects-all2', iAlgoRes.algo.short_name )+".gviz"
        vAllPaths = iAlgoRes.resAll
        if vAllPaths:
            output_paths.outputPaths( aOptions2, vOPathFilePath, vOGvizFilePath, vGraph, vCastTransaction, vAllPaths )



def kytProcessMain( aArgv ):
    if 0==len(aArgv) or not os.path.isfile( aArgv[0] ):
        logger.error( "***ERROR: No valid configuration file, exiting." )
        return
    vConfiguration = config.CConfig(aArgv[0])
    vConfiguration.processConfigurations(computeTransactionPath,None,True)



if __name__ == "__main__":
    logger.info( "Starting..." )
    vWatch = timewatch.TimeWatch()

    if "nt"==os.name or "win32"==sys.platform:
        aArgv = sys.argv[1:]
    else:
        # Mobile version
        aArgv = ( ( "/storage/emulated/0/Documents/_Lab/Py/Nuggets", None ), )

    kytProcessMain( aArgv )

    vWatch.stop()
    logger.info( "Finished: elapsed: {}, cpu: {}".format(vWatch.deltaElapsed(),vWatch.deltaCpu()) )    
    
    
    
    
    