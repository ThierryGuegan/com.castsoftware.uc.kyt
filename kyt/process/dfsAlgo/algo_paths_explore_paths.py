import collections
import time
import logging

logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.INFO
)

class InfiniteAlgoException(Exception):
    pass


PairNoiPath = collections.namedtuple( 'PairNoiPath', [ 'nbNoi', 'path'])

##== ------------------------------------------------------------------------
##== ------------------------------------------------------------------------
# Path info: ( Nb of program encounters, Nb of nodes in whishlist )
# CurrRes: ( 0: number of wishlist object visited so far in the current path, 1: path )
# aVisited: map: nodeNum to result: (0: nb of wishlist objects, 1: path )
# aAllPath: dict: node num -> all relevant paths from that node; node must not be a leaf
# New
# Return
#  None: no interesting paths from this node
# ( 0:(0: nbcv, 1:(shortest-path) ), 1:( 0:nbcv, 1:(longest-path) ), 2:( 0:nbvc, 1:(path-with-max-critical) ), 3:[ i:( 0:nbvc, 1:interesting path ) ]
def _explorePathsRec( aGraph, aNode, aNodesOfInterest, aTerminalNodes, aVisited=None, aCurrPath=tuple() ):
    retVal= None

    # Used to identify cached results
    if aVisited == None:
        aVisited = {}

    logger.debug( "> l{}: {}: {} [{}]".format(len(aCurrPath), aNode._num,aNode.formatNode(), aCurrPath) )

    if aNode._num not in aCurrPath:

        vCurrPath = aCurrPath + (aNode._num,)
        vNbNodeOfInterest = 1 if aNode._num in aNodesOfInterest else 0


        # Is current node end of path ie. current node in aTernminalNodes
        if aNode._num in aVisited:
            # Many path lead to this node, so retrieve cached values
            retVal = aVisited[aNode._num]
            return retVal

        elif aNode._num in aTerminalNodes:
            # Stop exploration for this path and return info about it
            retVal = ( ( vNbNodeOfInterest, (aNode._num,) ), ( vNbNodeOfInterest, (aNode._num,)), ( vNbNodeOfInterest, (aNode._num,)), [ (vNbNodeOfInterest,(aNode._num,)) ] )
            aVisited[aNode._num] = retVal
            logger.debug( "  -> terminal node found: {}: {}: {}".format(aNode._num,aNode.formatNode(),retVal) )

        else:
            if not aNode.isLeaf():
                logger.debug( "{} child nodes...".format(len(aNode._edges)) )
                vShortestChildPath = ( 0, None )
                vLongestChildPath = vCriticalestChildPath = ( -1, None )
                vAllPathOfInterest = []
                for iNodeNum2 in aNode._edges:
                    iNodeNum = int(iNodeNum2)
                    vRes = _explorePathsRec( aGraph, aGraph.node(iNodeNum), aNodesOfInterest, aTerminalNodes, aVisited, vCurrPath )
                    # Select shortest, longest and criticalest
                    if None != vRes:
                        # Update shortest path: shortest path with higher number of critical violations
                        vNbCvFromChild = vRes[0][0]
                        vPathFromChild = vRes[0][1]
                        if None!=vPathFromChild and ( ( vNbCvFromChild==vShortestChildPath[0] and ( None==vShortestChildPath[1] or len(vPathFromChild)<len(vShortestChildPath[1]) ) ) or vNbCvFromChild > vShortestChildPath[0] ):
                            vShortestChildPath = ( vNbCvFromChild, (aNode._num,)+vPathFromChild )
                            logger.debug( "updating shortest with: {}: {}: {}".format(aNode._num,vNbCvFromChild, (aNode._num,)+vPathFromChild) )
                        
                        # Update longest path: longest path with higher number of critical violations
                        vNbCvFromChild = vRes[1][0]
                        vPathFromChild = vRes[1][1]
                        if None!=vPathFromChild:
                            if ( vNbCvFromChild==vLongestChildPath[0] and ( None==vLongestChildPath[1] or len(vPathFromChild)>len(vLongestChildPath[1]) ) ) or vNbCvFromChild > vLongestChildPath[0]:
                                vLongestChildPath = ( vNbCvFromChild, (aNode._num,)+vPathFromChild )
                                logger.debug( "updating longest with: {}: {}: {}".format(aNode._num,vNbCvFromChild, (aNode._num,)+vPathFromChild) )

                        # Update criicalest path: path with higher number of critical violations
                        vNbCvFromChild = vRes[2][0]
                        vPathFromChild = vRes[2][1]
                        if vNbCvFromChild > vCriticalestChildPath[0]:
                            vCriticalestChildPath = ( vNbCvFromChild, (aNode._num,)+vPathFromChild )
                            logger.debug( "updating criticales with: {}: {}: {}".format(aNode._num,vNbCvFromChild, (aNode._num,)+vPathFromChild) )
                        
                        # add all paths
                        if len(vCurrPath) == 1:
                            for iP in vRes[3]:
                                logger.info( "{}: {}: {}".format(aNode._num, iNodeNum,str(iP)) )
                                vPathFromChild = iP[1]
                                vAllPathOfInterest.append( ( vNbNodeOfInterest+iP[0], (aNode._num,)+vPathFromChild ) )
                                #logger.info( "  -> new all paths: {}".format(vAllPathOfInterest) )
                
                if vShortestChildPath[0]>0:
                    vShortestChildPath = ( vShortestChildPath[0]+vNbNodeOfInterest, vShortestChildPath[1] )
                if vLongestChildPath[0]>0:
                    vLongestChildPath = ( vLongestChildPath[0]+vNbNodeOfInterest, vLongestChildPath[1] )
                if vCriticalestChildPath[0]>0:
                    vCriticalestChildPath = ( vCriticalestChildPath[0]+vNbNodeOfInterest, vCriticalestChildPath[1] )
                retVal = ( vShortestChildPath, vLongestChildPath, vCriticalestChildPath, vAllPathOfInterest )
                assert( aNode._num not in aVisited )
                aVisited[aNode._num] = retVal

            else:
                assert( aNode._num not in aTerminalNodes)
                # Path leading to this leaf is not interesting: will return None


    else:
        # Seems we have a cycle: and as we haven't stop exploration it means no interested node in this path, drop it
        #retVal = None
        pass

    return retVal


# ( 0:(0: nbcv, 1:(shortest-path) ), 1:( 0:nbcv, 1:(longest-path) ), 2:( 0:nbvc, 1:(path-with-max-critical) ), 3:[ i:( 0:nbvc, 1:interesting path ) ]
def _explorePaths2Rec( aGraph, aNode, aNodesOfInterest, aTerminalNodes, aVisited=None, aCurrPath=PairNoiPath(0,tuple() ) ):
    retVal= None

    # Used to identify cycles
    if aVisited == None:
        aVisited = {}
    #assert( aNode._num not in aVisited )
    logger.debug( "> l{}: {}: {} [{}]".format(len(aCurrPath.path), aNode._num,aNode.formatNode(), aCurrPath) )

    if aNode._num not in aCurrPath:

        vCurrPath = PairNoiPath(aCurrPath.nbNoi,aCurrPath.path + (aNode._num,))
        vNbNodeOfInterest = 1 if aNode._num in aNodesOfInterest else 0

        # Is current node end of path ie. current node in aTernminalNodes
        if aNode._num in aVisited:
            # Many path lead to this node, so retrieve cached values
            retVal = aVisited[aNode._num]
            return retVal

        elif aNode._num in aTerminalNodes:
            # Stop exploration for this path and return info about it
            retVal = ( ( vNbNodeOfInterest, (aNode._num,) ), ( vNbNodeOfInterest, (aNode._num,)), ( vNbNodeOfInterest, (aNode._num,)), [ (vNbNodeOfInterest,(aNode._num,)) ] )
            aVisited[aNode._num] = retVal
            logger.debug( "  -> terminal node found: {}: {}: {}".format(aNode._num,aNode.formatNode(),retVal) )

        else:
            if not aNode.isLeaf():
                logger.debug( "{} child nodes...".format(len(aNode._edges)) )
                vShortestChildPath = ( 0, None )
                vLongestChildPath = vCriticalestChildPath = ( -1, None )
                vAllPathOfInterest = []
                for iNodeNum2 in aNode._edges:
                    iNodeNum = int(iNodeNum2)
                    vRes = _explorePathsRec( aGraph, aGraph.node(iNodeNum), aNodesOfInterest, aTerminalNodes, aVisited, vCurrPath )
                    # Select shortest, longest and criticalest
                    if None != vRes:
                        # Update shortest path: shortest path with higher number of critical violations
                        vNbCvFromChild = vRes[0][0]
                        vPathFromChild = vRes[0][1]
                        if None!=vPathFromChild and ( ( vNbCvFromChild==vShortestChildPath[0] and ( None==vShortestChildPath[1] or len(vPathFromChild)<len(vShortestChildPath[1]) ) ) or vNbCvFromChild > vShortestChildPath[0] ):
                            vShortestChildPath = ( vNbCvFromChild, (aNode._num,)+vPathFromChild )
                            logger.debug( "updating shortest with: {}: {}: {}".format(aNode._num,vNbCvFromChild, (aNode._num,)+vPathFromChild) )
                        
                        # Update longest path: longest path with higher number of critical violations
                        vNbCvFromChild = vRes[1][0]
                        vPathFromChild = vRes[1][1]
                        if None!=vPathFromChild:
                            if ( vNbCvFromChild==vLongestChildPath[0] and ( None==vLongestChildPath[1] or len(vPathFromChild)>len(vLongestChildPath[1]) ) ) or vNbCvFromChild > vLongestChildPath[0]:
                                vLongestChildPath = ( vNbCvFromChild, (aNode._num,)+vPathFromChild )
                                logger.debug( "updating longest with: {}: {}: {}".format(aNode._num,vNbCvFromChild, (aNode._num,)+vPathFromChild) )

                        # Update criicalest path: path with higher number of critical violations
                        vNbCvFromChild = vRes[2][0]
                        vPathFromChild = vRes[2][1]
                        if vNbCvFromChild > vCriticalestChildPath[0]:
                            vCriticalestChildPath = ( vNbCvFromChild, (aNode._num,)+vPathFromChild )
                            logger.debug( "updating criticales with: {}: {}: {}".format(aNode._num,vNbCvFromChild, (aNode._num,)+vPathFromChild) )
                        
                        # add all paths
                        if len(vCurrPath) == 1:
                            for iP in vRes[3]:
                                logger.info( "{}: {}: {}".format(aNode._num, iNodeNum,str(iP)) )
                                vPathFromChild = iP[1]
                                vAllPathOfInterest.append( ( vNbNodeOfInterest+iP[0], (aNode._num,)+vPathFromChild ) )
                                #logger.info( "  -> new all paths: {}".format(vAllPathOfInterest) )
                
                if vShortestChildPath[0]>0:
                    vShortestChildPath = ( vShortestChildPath[0]+vNbNodeOfInterest, vShortestChildPath[1] )
                if vLongestChildPath[0]>0:
                    vLongestChildPath = ( vLongestChildPath[0]+vNbNodeOfInterest, vLongestChildPath[1] )
                if vCriticalestChildPath[0]>0:
                    vCriticalestChildPath = ( vCriticalestChildPath[0]+vNbNodeOfInterest, vCriticalestChildPath[1] )
                retVal = ( vShortestChildPath, vLongestChildPath, vCriticalestChildPath, vAllPathOfInterest )
                assert( aNode._num not in aVisited )
                aVisited[aNode._num] = retVal

            else:
                assert( aNode._num not in aTerminalNodes)
                # Path leading to this leaf is not interesting: will return None


    else:
        # Seems we have a cycle: and as we haven't stop exploration it means no interested node in this path, drop it
        #retVal = None
        pass

    return retVal


def explorePaths( aGraph, aNode, aNodesOfInterest, aTerminalNodes ):
    retVal = _explorePathsRec( aGraph, aNode, aNodesOfInterest, aTerminalNodes )
    return retVal