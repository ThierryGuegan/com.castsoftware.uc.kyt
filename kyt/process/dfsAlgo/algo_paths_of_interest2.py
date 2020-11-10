import collections
import time
import logging

logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.INFO
)

TExplorationData = collections.namedtuple( "TExplorationData", [ "g", "ooi", "loi", "startt"])


## -- -----------------------------------------------------------------------
# aG : graph to be explored (required to translate node <-> num )
# aStarNode : node from where to start exploration
# aOoI : list of object of interest's numbers
# aOoI : list of leaf of interest's numbers
# aVisited: set of object already visited, contains data computed duringprevious visit
# Return value:
#  None if subpath from startnode (incl. start node) is not relevant
#  tuple: for subpaths [ ( 0: nbr of leaf of interest, 1: number of object of interest, 2: subpath from StartNode), ... )
#TVisitedData = collections.namedtuple( "TVisitedData", [ "isOoI", "isLoI", "nbReachableLoi", "nbOoIUpstream", "nbOoIDownstream" ] )
TVisitedData2 = collections.namedtuple( "TVisitedData2", [ "isOoI", "isLoI", "reachableLoI", "ooIDownstream" ] )
#TExplorationData = collections.namedtuple( "TExplorationData", [ "g", "ooi", "loi"])
def _computeLoiAndOoIRec( aExplData, aStartNode, aVisited, aPath ):
    retVal = TVisitedData2(False,False,set(),set())
    logger.debug( "  exploring node: #{}".format(aStartNode._num) )

    if aStartNode._num not in aPath:
        if aStartNode._num in aVisited:
            # Node already visited: update visited data
            retVal = aVisited[aStartNode._num]

        else:
            isOoI = aStartNode._num in aExplData.ooi
            if aStartNode.isLeaf():
                isLoI = aStartNode._num in aExplData.loi
                retVal = TVisitedData2( isOoI, isLoI, set(), set() )
                aVisited[aStartNode._num] = retVal

            else:
                reachableLoI = set()
                vOoIDownstream = set()
                for iChild in aStartNode._edges:
                    vChildNode = aExplData.g.node(iChild)
                    vRes = _computeLoiAndOoIRec( aExplData, vChildNode, aVisited, aPath+(aStartNode._num,) )
                    if vRes.isLoI:
                        reachableLoI.add( iChild )    # in case child is leaf it is not counted in res
                    reachableLoI |= vRes.reachableLoI
                    if vRes.isOoI:
                        vOoIDownstream.add( iChild )   # in case child is leaf it is not counted in res
                    vOoIDownstream |= vRes.ooIDownstream
                retVal = TVisitedData2( isOoI, 0, reachableLoI, vOoIDownstream )
                aVisited[aStartNode._num] = retVal
    else:
        # Cycle detected: do nothing
        pass
    #assert( not(retVal.reachableLoI) )
    return retVal

def _pathsToObjectsOfInterestRec2( aExplData, aStartNode, isOoIUpstream, aVData, aVisited, aPath, aLevel=0 ):
    retVal = None

    # prevent too long exploration
    vElapsed = time.perf_counter()
    vDeltaT = vElapsed-aExplData.startt
    if vDeltaT > 60*5:
        logger.error( "***ERROR: exploration is taking too much time, aborting." )
        return None

    vVData = aVData[aStartNode._num]
    # Check whether we are going into a cycle or not
    logger.debug( "  {}- Exploring node: {}: {}".format('  '*aLevel,aStartNode._num,vVData) )
    if aStartNode._num not in aPath:
        if aStartNode._num not in aVisited:
            # Retrieve node's data

            if aStartNode.isLeaf():
                logger.debug( "  {}  leaf...".format('  '*aLevel) )
                if vVData.isLoI:
                    logger.debug( "  {}    leaf of interest".format('  '*aLevel) )
                    if vVData.isOoI:
                        retVal = [ aPath+(aStartNode._num,) ]
                        logger.debug( "  {}    leaf is also object of interest -> {}".format('  '*aLevel,retVal) )
                    
                    else:
                        #logger.info( "leaf: {}: {}".format(aStartNode._num,aPath) )
                        # leaf of interest => add path
                        #assert(false)
                        assert( isOoIUpstream )
                        retVal = [ aPath+(aStartNode._num,) ]
                        logger.debug( "  {}    leaf of interest -> {}".format('  '*aLevel,retVal) )

                elif vVData.isOoI:
                    retVal = [ aPath+(aStartNode._num,) ]
                    logger.debug( "  {}    leaf is object of interest -> {}".format('  '*aLevel,retVal) )
                aVisited[aStartNode._num] = True
            else:
                # Need to continue search if
                #   there is a Object of Interest ahead
                #   there is a Leaf of Interest and we have already crossed an Object of Interest
                if vVData.ooIDownstream : #or ( vVData.reachableLoI and ( isOoIUpstream or vVData.isOoI ) ):
                    # Need to continue
                    retVal = []
                    for iChild in aStartNode._edges:
                        vChildNode = aExplData.g.node(iChild)
                        vRes = _pathsToObjectsOfInterestRec2( aExplData, vChildNode, isOoIUpstream or vVData.isOoI, aVData, aVisited, aPath+(aStartNode._num,), aLevel+1 )
                        if vRes:
                            retVal.extend( vRes )
                
                elif vVData.isOoI:
                    # path of interest ending here 
                    retVal  = [ aPath+(aStartNode._num,) ]
                aVisited[aStartNode._num] = True
        else:
            # end path here: rest of the path already been selected
            if vVData.isOoI or vVData.ooIDownstream:
                retVal = [ aPath+(aStartNode._num,) ]
                logger.debug( "  {}  already visited -> {}".format('  '*aLevel,retVal) )
                assert( vVData.isOoI or vVData.ooIDownstream )

    else:
        # cycle detected:
        logger.debug( "  {}  cycle detected -> {}".format('  '*aLevel,retVal) )

        pass

    return retVal

def pathsToObjectsOfInterest2( aGraph, aStartNode, aObjectsOfInterest, aLeafOfInterest, aAllEndPoints=False ):
    logging.getLogger().setLevel(logging.INFO)
    vExplData = TExplorationData(aGraph,aObjectsOfInterest, set() if True else aLeafOfInterest,time.perf_counter())
    vVisitData = {}
    vRes = _computeLoiAndOoIRec( vExplData, aStartNode, vVisitData, tuple() )
    logger.info( "  computed data: isLoI: {}, isOoI: {}, nb reachable leaves: {}, nb ooi downstream: {}".format(
        vRes.isLoI, vRes.isOoI, len(vRes.reachableLoI), len(vRes.ooIDownstream)
    ) )

    vRes = _pathsToObjectsOfInterestRec2( vExplData, aStartNode, False, vVisitData, {}, tuple() )
    # keep only pathes where at least one object of interest has been crossed
    #retVal = [ x for x in res if aAllEndPoints or x[1]>0 ]
    retVal = [ x for x in vRes ] if vRes else []
    logging.getLogger().setLevel(logging.INFO)
    return retVal