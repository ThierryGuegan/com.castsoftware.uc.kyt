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



##== ------------------------------------------------------------------------
# Path info: ( Nb of program encounters, Nb of nodes in whishlist )
# CurrRes: ( 0: number of wishlist object visited so far in the current path, 1: path )
# aVisited: map: nodeNum to result: (0: nb of wishlist objects, 1: path )
# aAllPath: dict: node num -> all relevant paths from that node; node must not be a leaf
# Used
def _findPathesStopAtTerminalRec( aGraph, aNode, aWishList, aWishLeaves, aAllPaths, aWithCycles, aVisited=None, aCurrPath=tuple() ):
    retVal= None

    if aVisited == None:
        aVisited = {}
    assert( aNode._num not in aVisited )

    vCurrPath = aCurrPath + (aNode._num,)

    if aNode._num not in aCurrPath:
        # is node in wish list ?
        vInWishList = 1 if ( aNode._num in aWishList ) else 0

        if aNode.isLeaf() or ( aNode._num in aWishLeaves and (aNode._obj._object_type=="Oracle table" or aNode._obj._object_type=="Table") ):
            # interested in paths leading to a table (to extend to endpoints?)
            if ( not aWishLeaves ) or ( aNode._num in aWishLeaves ):
                logger.debug( "Leaf of interest: {}, inWishList: {}, path: {}".format(aNode.formatNode(),vInWishList,vCurrPath) )
                retVal = ( vInWishList, vCurrPath )
                assert( None!=retVal and None!=retVal[1] )
            else:
                # paths leading to this leaf is not relevant
                retVal = None

        else:
            vTmpRet = ( 0, None )

            for iNodeNum in aNode._edges:
                if int(iNodeNum) in aVisited:
                    # already visited : use cached value and update path
                    vRes = aVisited[int(iNodeNum)]
                    if None!=vRes and None!=vRes[1]:
                        #path need to be updated cause path that led to node is different from the one cached, only path after node is correct
                        vPosNodeInCachedPath = vRes[1].index(int(iNodeNum))
                        vNewPath = vCurrPath + vRes[1][vPosNodeInCachedPath:]
                        vRes = ( vRes[0], vNewPath )
                else:
                    vRes = _findPathesStopAtTerminalRec( aGraph, aGraph.node(iNodeNum), aWishList, aWishLeaves, aAllPaths, aWithCycles, aVisited, vCurrPath )
                    assert( (vRes == None) or (vRes[1]==None) or ( int(iNodeNum) in vRes[1]) )
                
                if vRes!=None:
                    if vRes[0] > vTmpRet[0]:
                        vTmpRet = vRes
                        assert( vTmpRet[1] != None )
                        
                    elif vRes[0]==vTmpRet[0]:
                        if vTmpRet[1]!=None:
                            if vRes[1] != None:
                                if len(vRes[1]) > len(vTmpRet[1]):
                                    vTmpRet = vRes
                                    assert( vTmpRet[1] != None )
                        else:
                            if vRes[1] != None:
                                vTmpRet = vRes
                                assert( vTmpRet[1] != None )

                    # is path relevant and to be added to all paths ? 
                    if None!=aAllPaths and ( (vRes[0]>0 or vInWishList) and vRes[1]!=None ):
                        logger.debug( "  relevant path: {}: {}".format(aNode.formatNode(),vRes[1]) )
                        if int(aNode._num) in aAllPaths:
                            aAllPaths[int(aNode._num)].append( (vRes[0]+vInWishList, vRes[1] ) )
                        else:
                            aAllPaths[int(aNode._num)] = [ (vRes[0]+vInWishList, vRes[1] ) ]
                else:
                    # vRes == None
                    pass

            if vInWishList and vTmpRet[1] !=None:
                # update count of wishlist nodes encountered (vTempRet[0])
                # not interested in paths that do not lead to DB (vTmpRet[1]==None)
                vTmpRet = ( vTmpRet[0]+1, vTmpRet[1] )
            
            retVal = vTmpRet
            if vTmpRet == ( 0, None ):
                retVal = None

        aVisited[int(aNode._num)] = retVal
        assert( retVal==None or retVal[1]==None or aNode._num in retVal[1])

    else:
        # cycle detected => end the exploration and no relevant result - TODO: include cycles when cause by DB objects : potential misses
        if aWithCycles:
            # Consider node as leaf and take it into account if relevant
            # interested in paths leading to a table (to extend to endpoints?)
            if (not aWishLeaves) or aNode._num in aWishLeaves:
                # Dont test if object in wish list: as there is a cycle that will be done upward; same for cached value
                retVal = ( 0, vCurrPath )
                assert( None!=retVal and None!=retVal[1] )
        else:
            pass


    return retVal

# Used
def findPathesStopAtTerminal( aGraph, aRootNode, aWishObjects, aWishLeaves, aCycles=False ):
    retAllPaths = { aRootNode._num : [] }
    retRes = _findPathesStopAtTerminalRec( aGraph, aRootNode, aWishObjects, aWishLeaves, retAllPaths, aCycles )
    return retRes, retAllPaths
