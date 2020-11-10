import collections
import time
import logging

logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.INFO
)


def pathsToObjectsOfInterest( aGraph, aStartNode, aObjectsOfInterest, aLeafOfInterest, aAllEndPoints=False ):
    logging.getLogger().setLevel(logging.INFO)
    try:
        res = _pathsToObjectsOfInterestRec( TExplorationData(aGraph,aObjectsOfInterest, aLeafOfInterest,time.perf_counter()),
            aStartNode, -1 if aAllEndPoints else 0, {}, tuple() )
    except ( AssertionError, TypeError, NameError, AttributeError ):
        raise
    except:
        res = None

    
    # keep only pathes where at least one object of interest has been crossed
    if res:
        retVal = [ x for x in res if aAllEndPoints or x[1]>0 ]
    else:
        retVal = []

    logging.getLogger().setLevel(logging.INFO)
    return retVal


## -- -----------------------------------------------------------------------
# aG : graph to be explored (required to translate node <-> num )
# aStarNode : node from where to start exploration
# aOoI : list of object of interest's numbers
# aOoI : list of leaf of interest's numbers
# aVisited: set of object already visited, contains data computed duringprevious visit
# Return value:
#  None if subpath from startnode (incl. start node) is not relevant
#  tuple: for subpaths [ ( 0: nbr of leaf of interest, 1: number of object of interest, 2: subpath from StartNode), ... )
TVisitData = collections.namedtuple( "TVisitData", [ "resSubpath", "nbAscOoi" ] )
TExplorationData = collections.namedtuple( "TExplorationData", [ "g", "ooi", "loi", "startt"])
def _pathsToObjectsOfInterestRec( aExplData, aStartNode, aNbAscOoI, aVisited, aPath ):
    isOoI = 1 if aStartNode._num in aExplData.ooi else 0
    logger.debug( "  exploring node: #{}: {}: nbAscOoi: {}, isOoI: {}".format(aStartNode._num,aStartNode._obj._object_id,aNbAscOoI,isOoI) )

    # prevent too long exploration
    vElapsed = time.perf_counter()
    vDeltaT = vElapsed-aExplData.startt
    if vDeltaT > 60*5:
        logger.error( "***ERROR: exploration is taking too much time, aborting." )
        raise  Exception("***ERROR: exploration is taking too much time, aborting.")

    if aStartNode._num not in aPath:
        if aStartNode._num in aVisited:
            # use cached data when available
            retVal = aVisited[aStartNode._num]
            retVal = None
            retVal = aVisited[aStartNode._num]

        elif aStartNode.isLeaf() or aStartNode._num in aExplData.loi:
            if aStartNode._num in aExplData.loi:
                # Leaf is leaf of interest
                if isOoI:
                    # and Object of interest => cached value
                    retVal =  [ ( 1, 1, (aStartNode._num,) ) ]
                
                elif -1==aNbAscOoI or aNbAscOoI>0:
                    retVal =  [ ( 1, 0, (aStartNode._num,) ) ]
                
                else:
                    # not of interest for the present path but might be for others => cached non None value
                    #if (isLoI and ( -1==aNbAscOoI or aNbAscOoI>0 ) ) or isOoI:
                    retVal =  [ ( 1, 0, (aStartNode._num,) ) ]
            elif isOoI:
                retVal =  [ ( 0, 1, (aStartNode._num,) ) ]
            
            else:
                # Not leaf of interest, nor object of interest => will never be => cache None value
                retVal =  None
            aVisited[aStartNode._num] = retVal
            
        else:
            retVal = []
            logger.debug( "    with {}child nodes".format(len(aStartNode._edges)) )
            for iChild in aStartNode._edges:
                vChildNode = aExplData.g.node(iChild)
                # 1) Path from child doesn't contains any OoI nor does it lead to leaf of interest:
                #   drop sub path unless start node is of interest
                # 2) Path from child lead to leaf of interest:
                #   update number of object of interest, and path
                vNbAscOoI = ( aNbAscOoI + isOoI ) if -1 != aNbAscOoI else -1
                vRes = _pathsToObjectsOfInterestRec( aExplData, vChildNode, vNbAscOoI, aVisited, aPath+(aStartNode._num,) )
                if None != vRes:
                    # there is/are subpath/es of interest: update data
                    for iSubpath in vRes:
                        logger.debug( "    with {} subpathes".format(len(vRes)) )
                        #logger.info( "subpath: {}".format(iSubpath) )
                        retVal.append( ( iSubpath[0], iSubpath[1]+isOoI, (aStartNode._num,)+iSubpath[2] ) )

            if 0 == len(retVal):
                if isOoI:
                    retVal = [ ( 0, 1, ( aStartNode._num,) ) ]          
                else:
                    retVal = None

            aVisited[aStartNode._num] = retVal
    else:
        # cycle detected:
        #   ignore it (too complicated to handle properly all cases)
        retVal = None
    return retVal


