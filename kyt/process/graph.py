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

from common import timewatch
from common import config 
from dal import n_dal
from common import n_graph
from model import n_data
from render import gviz

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


def formatPath( aBaseFilePath, aFile, aIndex=-1 ):
    if aIndex >= 0:
        return os.path.join( aBaseFilePath, "{0}-{1:0>2}.{2}".format(
            gFileNames[aFile][0], aIndex, gFileNames[aFile][1] ) )
    else:
        return os.path.join( aBaseFilePath, "{0}.{1}".format(
            gFileNames[aFile][0], gFileNames[aFile][1] ) )


def accessibleNodesRec( aG, aNd ):
    retVal = tuple()

    if aNd._tag == -1:
        aNd._tag = 0 # open node
        retVal += ( aNd._num, )
        if not aNd.isLeaf():
            for iNum in aNd._edges:
                vRes = accessibleNodesRec( aG, aG.node(iNum) )    
                retVal += vRes
        aNd._tag = 1 # close node
               
    return retVal
    
def accessibleNodes( aG, aNodeFrom, vSet1=None, vSet2=None ):
    aG.tagAll( -1 ) # all are free
    vRes = accessibleNodesRec(aG, aNodeFrom)
    vRes1 = { x for x in vRes }
    vRes2 = { x._num for x in aG._nodes if x._num not in vRes1 }

    vNbInSet1 = 0
    vNbInSet2 = 0
    for eNum in vRes1:
        if None!=vSet1 and eNum in vSet1:
            vNbInSet1 += 1
        if None!=vSet2 and eNum in vSet2:
            vNbInSet2 += 1
    return ( vRes1, vRes2, vNbInSet1, vNbInSet2 )

# Level:
#  0 : numeric identifier only (index in graph node list)
#  1 : 0 + CAST object id
#  2 : 1 + CAST object name
#  3 : 2 + CAST object type
#  x : 3 + CAST object fullname
def formatNode( aNode, aLevel=3 ):
    retVal = None
    if aNode == None:
        retVal = "<None>"
    else:
        if aLevel == 0:
            retVal = "{{ num: {0:>4} }}".format( aNode._num )
        elif aLevel == 1:
            retVal = "{{ num: {0:>4}, object_id: {1:>8} }}".format( aNode._num, aNode._obj._object_id )
        elif aLevel == 2:
            retVal = "{{ num: {0:>4}, object_id: {1:>8}, object_name: {2} }}".format( aNode._num, aNode._obj._object_id, aNode._obj._object_name )
        elif aLevel == 3:
            retVal = "{{ num: {0:>4}, object_id: {1:>8}, object_name: {2}, object_type: {3} }}".format( aNode._num, aNode._obj._object_id, aNode._obj._object_name, aNode._obj._object_type )
        else:
            retVal = "{{ num: {0:>4}, object_id: {1:>8}, object_name: {2}, object_type: {3}, object_fullname: {4} }}".format(\
                aNode._num, aNode._obj._object_id, aNode._obj._object_name, aNode._obj._object_type, aNode._obj._object_fullname )
    return retVal

def objectsWithNoCaller( aGraph ):
    vWithoutCallers = { x for x in range(len(aGraph._nodes)) }
    
    for iNode in aGraph._nodes:
        for iCalledNum in iNode._edges:
            if iCalledNum in vWithoutCallers:
                vWithoutCallers.remove(iCalledNum)
    return vWithoutCallers

def dumpGraph( aG, aFolderPath ):
    vFilePath = os.path.join(aFolderPath,"z_dump_graph.txt" )
    with open( vFilePath, "w") as vFile:
        print( "#0/A:CallerObjectId|1/B:CalleeObjectId|2/C:CallerObjectName|3/D:CalleeObjectName|4/E:CallerObjectFullname|5/F:CalleeObjectFullname", file=vFile )
        for iNode in aG._nodes:
            for iNum in iNode._edges:
                vNode = aG.node(iNum)
                print( iNode._obj._object_id,'|',vNode._obj._object_id,'|',
                    iNode._obj._object_name,'|',vNode._obj._object_name,'|',iNode._obj._object_fullname,'|',vNode._obj._object_fullname,
                    file=vFile
                )


def outputPath2( aBaseFilePath, aLabel, aResult, aGenerateDot, aGraph, aTransactionId, aTransactionName, aPath, aObjectsOfInterest, aObjectsWithV ):
    vFilePath = formatPath( os.path.join(aBaseFilePath,"_paths"), 'enlighten-objects', aResult )
    vGvizFilePath = formatPath( os.path.join(aBaseFilePath,"_gviz"), 'enlighten-objects', aResult )+".gviz"
    logger.info( "Writing path in file [{0}]...".format(vFilePath) )
    logger.info( "Writing file [{0}]...".format(vGvizFilePath) )
    with open( vFilePath,"w" ) as vF, open( vGvizFilePath, "w") as vFG:
        outputPath( vF, vFG, aGenerateDot, aGraph, aTransactionId, aTransactionName, aPath, aObjectsOfInterest, aObjectsWithV )

def outputPath( aF, aFG, aGenerateDot, aGraph, aTransactionId, aTransactionName, aPath, aObjectsOfInterest, aObjectsWithV ):
    vObjectsInPath = set()
    vTransactionSubfolder = os.path.basename(aBaseFilePath)
    
    vLine = "# 0/A: Critical | 1/B: TransactionId | 2/C: TransactionName | 3/D: ObjectId | 4/E: ObjectName | 5/F: ObjectFullname | 6/G: MetricId | 7/H: MetricName | 8/I: ObjectType"
    print( vLine, file=aF )

    # GViz header
    print( "digraph {\n", file=aFG)
    print( '    labelloc="t"; label="{}: {}";'.format(vTransactionSubfolder,aTransactionName), file=vGvizFile )
    print( '    ranksep=1.25;', file=aFG)
    print( '    node [shape=none, fontname="Arial"];', file=aFG)
    print( '    graph [fontname="Arial"];', file=aFG)
    print( '    edge [fontname="Arial"];', file=aFG)

    if None!=aPath and None!=aPath[1]:
        for iIdx, iNum in enumerate(aPath[1]):
            vIsFirst = iIdx==0
            vIsLast = iIdx==len(aPath[1])-1
            vNode = aGraph.node(iNum)
            vLine = ''+str(aTransactionId)+'|'+aTransactionName+'|'+vNode._obj._object_id+'|'+vNode._obj._object_name+'|'+vNode._obj._object_fullname
            vObjectsInPath.add( int(iNum) )
            vCViolations = []
            vViolations = []
            if vNode._num in aObjectsWithV and len(aObjectsWithV[int(vNode._num)])>0 :
                vObjWithV = aObjectsWithV[int(vNode._num)]
                vLastMId = set()
                for iO in vObjWithV:
                    if iO._metric_id not in vLastMId:
                        if iO._is_critical:
                            vLine_ = '*|'
                            vCViolations.append( "* {}".format(iO._metric_name) )
                        else:
                            vLine_ = '-|'
                            vViolations.append( "  {}".format(iO._metric_name) )
                        vLine_ += vLine + '|' + str(iO._metric_id) + '|' + iO._metric_name
                        vLine_ += '|'+vNode._obj._object_type
                        print( vLine_, file=aF )
                        vLastMId.add( iO._metric_id )
            else:
                vLine = ' |' + vLine + "|-|-"
                vLine += '|'+vNode._obj._object_type
                print( vLine, file=aF )

            # outputing gviz: need to output the links only for object present in path
            vGvizCViolations = ""
            vGvizViolations = ""
            if len(vCViolations)>0 :
                #vGvizCViolations =  '<font color="red4" point-size="10"><br/>'+'<br/>'.join(str(x).replace("<","&lt;").replace(">","&gt;") for x in vCViolations)+'</font>'
                vGvizCViolations =  '<font color="red4"><br/>'+'<br/>'.join(str(x).replace("<","&lt;").replace(">","&gt;") for x in vCViolations)+'</font>'
            
            if len(vViolations)>0:
                # Output only 6 violations at max
                #vGvizViolations =  '<font color="gray20" point-size="10"><br/>'+'<br/>'.join(str(x).replace("<","&lt;").replace(">","&gt;") for x in vViolations[:6])+'</font>'
                vGvizViolations =  '<font color="gray20"><br/>'+'<br/>'.join(str(x).replace("<","&lt;").replace(">","&gt;") for x in vViolations[:6])+'</font>'
            vGvizLabel = '<<u><font color="blue3">{}</font></u><br/><font color="magenta3">{}</font>{}{}>'.format(
                vNode._obj._object_type,
                vNode._obj._object_name.replace("<","&lt;").replace(">","&gt;"),
                vGvizCViolations, vGvizViolations
            )
            vGVizNodeShape = 'color="white", shape=box,' if 0==len(vCViolations) else "shape=box, "
            if vIsFirst or vIsLast:
            #    vGVizNodeShape = "style=rounded, shape=box, "
                vGVizNodeShape = 'color="orangered1", shape=box, '
            vGvizNode = '"{}" [{}label={}];'.format(
                vNode._obj._object_id, vGVizNodeShape, vGvizLabel
            )
            print( vGvizNode, file=aFG )

        print( "", file=aFG )

        for iNdNum in vObjectsInPath:
            for iLNum in aGraph._nodes[iNdNum]._edges:
                if iLNum in vObjectsInPath:
                    print( '  "{0}"  -> "{1}";'.format(
                        aGraph._nodes[iNdNum]._obj._object_id,aGraph._nodes[iLNum]._obj._object_id),
                        file=aFG
                    )
        print( "\n}", file=aFG )

# return True if to keep
def filterAgainstHF( aHealthFactors, aViolation ):
    retVal = ( None == aHealthFactors ) or ( 0 == len(aHealthFactors) ) or ( int(aViolation._b_criterion) in aHealthFactors )
    logger.info( "    Filter: healthfactors: {} -> {}".format(aHealthFactors,retVal) )
    return retVal



def outputPathes( aOutputFilePath, aGenerateDot, aGraph, aTransactionId, aTransactionName, aAllPaths, aRootNode, aObjectViolations, aHealthFactors=None ):
    vObjectsInPath = set()
    with open( aOutputFilePath,"w") as vFile:
        logger.info( "Writing path in file [{0}]...".format(aOutputFilePath) )
        for vLine in ( "#All# 0/A: Critical | 1/B: NbPaths | 2/C: PathNum | 3/D: PathLen | 4/E: StepNum | 5/F: TransactionId | 6/G: TransactionName | 7/H: ObjectId | 8/I: ObjectName | 9/J: ObjectFullname | 10/K: MetricId | 11/L: MetricName",
            "#{{ NbPaths: {0}, TransactionId: {1}, TransactionName='{2}' }}".format(len(aAllPaths[aRootNode._num]),aTransactionId,aTransactionName)
        ):
            print( vLine, file=vFile )
        
        vPath = 0
        for iPath in aAllPaths[aRootNode._num]:
            vStep = 0
            for iNum in iPath[1]:
                vNode = aGraph.node(iNum)
                vLine = "|{0}|{1}|{2}|{3}|{7}|{8}|{4}|{5}|{6}|".format(
                    len(aAllPaths[aRootNode._num]), vPath, len(iPath[1]), vStep,                 # 0/1/2/3
                    vNode._obj._object_id, vNode._obj._object_name, vNode._obj._object_fullname, # 4/5/6
                    aTransactionId, aTransactionName                                                # 7/8
                )
                vObjectsInPath.add( iNum )
                if vNode._num in aObjectViolations and len(aObjectViolations[int(vNode._num)])>0 :
                    vObjWithV = aObjectViolations[int(vNode._num)]
                    vLastMId = set()
                    for iO in vObjWithV:
                        if iO._metric_id not in vLastMId:
                            if iO._is_critical:
                                vLine_ = '*' + vLine + str(iO._metric_id) + '|' + iO._metric_name
                            else:
                                vLine_ = '-' + vLine + str(iO._metric_id) + '|' + iO._metric_name
                            print( vLine_, file=vFile )
                            vLastMId.add( iO._metric_id )

                else:
                    vLine_ = ' ' + vLine + "-|-"
                    print( vLine_, file=vFile )
                vStep += 1

            vPath += 1
    return




def loadFacts( aBaseFilePath, aOptions ):
    retVal = { "_objects":None, "_graph":None, "_endPoints":None, "_objectsWithCV":None, "_objectsWithV":None }

    vTrObjectsFilePath = formatPath( aBaseFilePath, 'transaction-objects' )
    logger.info( "Loading transaction object from [{}]...".format(vTrObjectsFilePath) )
    vObjects = n_dal.loadObjects( vTrObjectsFilePath )
    retVal["_objects"] = vObjects

    vTrGraphFilePath = formatPath( aBaseFilePath, 'transaction-links' )
    logger.info( "Loading transaction links from [{}]...".format(vTrGraphFilePath) )
    vGraph = n_dal.loadGraph( vTrGraphFilePath, vObjects )
    retVal["_graph"] = vGraph

    vTrEndPointsFilePath = formatPath( aBaseFilePath, 'transaction-endpoints' )
    if os.path.isfile( vTrEndPointsFilePath ):
        logger.info( "Loading end points from [{}]...".format(vTrEndPointsFilePath) )
        vEndpoints = n_dal.loadTransactionEndpoints( vTrEndPointsFilePath )
        retVal["_endPoints"] = vEndpoints
    else:
        logger.info( "No end points file [{}]...".format(vTrEndPointsFilePath) )
        retVal["_endPoints"] = []


    vTrObjectsWCvFilePath = formatPath( aBaseFilePath, 'object-critical-violations' )
    logger.info( "Loading objects with critical violations from [{}]...".format(vTrObjectsWCvFilePath) )
    vObjectsWithVC = {}
    vObjectsNum = None
    if True == aOptions.get("withCriticalViolations"):
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



def outputGraph( aGraph, aBaseFilePath, aTransactionName, aEntryPointNum, aEndPointNums, aObjectsWithCV, aObjectWithV ):
    vTransactionSubfolder = os.path.basename(aBaseFilePath)
    C_COLOR_ORANGERED1="orangered1"
    C_COLOR_BLUE3="blue3"
    C_COLOR_MAGENTA3="magenta3"
#    ranksep = 1.25;
    C_GVIZ_HEADER = (""+
    'digraph {{\n'+
    '    labelloc="t"; label="{}: {}";\n'+
    '    node [shape=none, fontname="Arial"];\n'+
    '    graph [fontname="Arial"];\n'+
    '    edge [fontname="Arial"];').format(vTransactionSubfolder,aTransactionName)

    # Arguments: object id, box color, object type, object name
    C_GVIZ_VERTEX = '"{}" [color="orangered1", shape=box, label=<<u><font color="blue3">{}</font></u><br/><font color="magenta3">{}</font>{}>];'
    C_GVIZ_VERTEX2 = '"{}" [color="white", shape=box, label=<<u><font color="blue3">{}</font></u><br/><font color="magenta3">{}</font>{}>];'
    C_GVIZ_VERTEX_CV = '"{}" [color="red", shape=box, label=<<u><font color="blue3">{}</font></u><br/><font color="magenta3">{}</font>{}>];'
    C_GVIZ_VERTEX_V = '"{}" [color="gray30", shape=box, label=<<u><font color="blue3">{}</font></u><br/><font color="magenta3">{}</font>{}>];'
    C_GVIZ_EDGE = '  "{}" -> "{}";'
    C_GVIZ_TAIL = "\n}"

    vWithNumAndId = True
    with open( os.path.join(aBaseFilePath,"_gviz","99_graph.gvz"), "w" ) as vOF:
        # output prolog
        print( C_GVIZ_HEADER, file=vOF )

        # output vertices
        for x in aGraph._nodes:
            vNumAndId = '<br/><font color="gray20">#{}: {}</font>'.format(x._num,x._obj._object_id) if vWithNumAndId else ""
            vLabelType = x._obj._object_type.replace("<","&lt;").replace(">","&gt;")
            vLabelName = x._obj._object_name.replace("<","&lt;").replace(">","&gt;")
            if x._num==aEntryPointNum or x._num in aEndPointNums:
                print( C_GVIZ_VERTEX.format(x._obj._object_id,vLabelType,vLabelName,vNumAndId), file=vOF )
            elif x._num in aObjectsWithCV:
                print( C_GVIZ_VERTEX_CV.format(x._obj._object_id,vLabelType,vLabelName,vNumAndId), file=vOF )
            elif x._num in aObjectWithV:
                print( C_GVIZ_VERTEX_V.format(x._obj._object_id,vLabelType,vLabelName,vNumAndId), file=vOF )
            else:
                print( C_GVIZ_VERTEX2.format(x._obj._object_id,vLabelType,vLabelName,vNumAndId), file=vOF )

        # output edges
        print( "", file=vOF )
        for x in aGraph._nodes:
            for y in x._edges:
                vY = aGraph.node( y )
                print( C_GVIZ_EDGE.format(x._obj._object_id,vY._obj._object_id), file=vOF )

        # output tail
        print( C_GVIZ_TAIL, file=vOF )


def computeGraph( aOptions ):
    vBaseFilePath = aOptions['tr-output-folder']
    logger.info( "-- Processing transaction [{0}]".format(vBaseFilePath) )

    vTransactionId, vTransactionName = getTransactionInfo(os.path.join(vBaseFilePath,"_data"))

    vFacts = loadFacts(os.path.join(vBaseFilePath,"_data"), aOptions)

    vObjects = vFacts["_objects"]
    vGraph = vFacts["_graph"]
    vEndpoints = vFacts["_endPoints"]

    vEndpointNums = { vGraph.numFromObj(x) for x in vEndpoints if None!=vGraph.numFromObj(x) }
    vTableObjects = { x._num for x in vGraph._nodes if ( x._obj._object_type=="Oracle table" or x._obj._object_type=="Table" ) }

    logger.info( "  nb of transaction endpoints: {}".format(len(vEndpointNums)) )
    logger.info( "  nb of transaction tables: {}".format(len(vTableObjects)) )
    
    # merge table and endpoints
    for x in vTableObjects:
        vEndpointNums.add( x )

    vObjectsWithVC = vFacts["_objectsWithCV"]
    vObjectsNum = vFacts["_objectsWithCVNum"]
    vObjectsWithV = vFacts["_objectsWithV"]

    dumpGraph( vGraph, vBaseFilePath )

    # Retrieve root node
    vRootNodeId = aOptions['transaction']['root-object-id'] if 'root-object-id' in aOptions['transaction'] else None
    if vRootNodeId != None:
        vRootNode = vGraph.nodeFromObj( vRootNodeId )
    else:
        vRootNode = None

    vNodesWithoutCallers = objectsWithNoCaller(vGraph)
    logger.info( "Objects without callers: {}: {}".format(len(vNodesWithoutCallers),vNodesWithoutCallers) )
    vMaxAccessibles = 0 
    vRealRootNode = vRootNode
    for iNum in vNodesWithoutCallers:
        vNode = vGraph.node(iNum)
        vRes5b = accessibleNodes( vGraph, vNode, vTableObjects, vEndpointNums )
        if len(vRes5b[0]) > vMaxAccessibles:
            vRealRootNode = vNode
            vMaxAccessibles = len(vRes5b[0])

    if None == vRootNodeId:
        logger.info( ">>>>>> Using node {0} as root node instead of {1}".format(formatNode(vRealRootNode),formatNode(vRootNode)) )
        vRootNode = vRealRootNode
    
    if None == vRealRootNode:
        logger.warning( "!!!WARNING: Could not find any node without callers ! Skipping." )
        return

    logger.info( "-- Root object:\n  ".format(formatNode(vRootNode)) )
    vRes5 = accessibleNodes( vGraph, vRootNode, vTableObjects, vEndpointNums )
    logger.info( "  -> Accessibles nodes: {0} out of {1}: {2}".format(len(vRes5[0]),len(vGraph._nodes),formatNode(vRootNode)) )
    logger.info( "  ->   accessibles tables: {0}".format(vRes5[2]) )
    logger.info( "  ->   accessibles endpoints: {0}".format(vRes5[3]) )
    vNum = 0
    for iNum in vRes5[1]:
        vNum += 1
        vNode = vGraph.node(iNum)
        logger.info( "  object_id: {:>8}, name: {}, fullpath: {}".format(vNode._obj._object_id,vNode._obj._object_name,vNode._obj._object_fullname ) )
        if vNum>6:
            break

    outputGraph( vGraph, vBaseFilePath, vTransactionName, vRootNode._num, vEndpointNums, vObjectsWithVC, vObjectsWithV ) #None, None )

def kytGraphMain( aArgv ):
    if 0==len(aArgv) or not os.path.isfile( aArgv[0] ):
        logger.error( "***ERROR: No valid configuration file, exiting." )
        return 1
    vConfiguration = config.CConfig(aArgv[0])
    vConfiguration.processConfigurations(computeGraph,None,True)
    return 0



if __name__ == "__main__":
    logger.info( "Starting..." )

    if "nt"==os.name or "win32"==sys.platform:
        aArgv = sys.argv[1:]
    else:
        # Mobile version
        aArgv = ( ( "/storage/emulated/0/Documents/_Lab/Py/Nuggets", None ), )

    kytGraphMain( aArgv )

    logger.info( "Finished." )
    
    
    
    
    
    