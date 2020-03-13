# Copyright (C) 2020 You-Cast on Earth, Moon and Mars 2020
# This file is part of com.castsoftware.uc.kyt extension
# which is released under GNU GENERAL PUBLIC LICENS v3, Version 3, 29 June 2007.
# See file LICENCE or go to https://www.gnu.org/licenses/ for full license details.

class GraphNode:
    __slots__ = '_num', '_tag', '_obj', '_edges'

    def __init__( self, aNum, aCastObject, aTag=-1 ):
        self._num = aNum
        self._obj = aCastObject
        self._edges = set()
        self._tag = aTag

    def isLeaf( self ):
        return 0==len(self._edges)

    # Level:
    #  0 : numeric identifier only (index in graph node list)
    #  1 : 0 + CAST object id
    #  2 : 1 + CAST object name
    #  3 : 2 + CAST object type
    #  x : 3 + CAST object fullname
    def formatNode( self, aLevel=3 ):
        retVal = None
        if aLevel == 0:
            retVal = "{{ num: {0:>4} }}".format( self._num )
        elif aLevel == 1:
            retVal = "{{ num: {0:>4}, object_id: {1:>8} }}".format( self._num, self._obj._object_id )
        elif aLevel == 2:
            retVal = "{{ num: {0:>4}, object_id: {1:>8}, object_name: {2} }}".format( self._num, self._obj._object_id, self._obj._object_name )
        elif aLevel == 3:
            retVal = "{{ num: {0:>4}, object_id: {1:>8}, object_name: {2}, object_type: {3} }}".format( self._num, self._obj._object_id, self._obj._object_name, self._obj._object_type )
        else:
            retVal = "{{ num: {0:>4}, object_id: {1:>8}, object_name: {2}, object_type: {3}, object_fullname: {4} }}".format(\
                self._num, self._obj._object_id, self._obj._object_name, self._obj._object_type, self._obj._object_fullname )
        return retVal


class Graph:
    __slots__ = '_nodes', '_mapObj2Num'

    def __init__( self ):
        self._nodes = []
        self._mapObj2Num = {}

    def node( self, aNum ):
        return self._nodes[int(aNum)]
    
    def edges( self, aNum ):
        return self._nodes[int(aNum)]._edges

    def addNode( self, aCastObject ):
        vNewGraphNode = GraphNode( len(self._nodes), aCastObject )
        self._nodes.append( vNewGraphNode )
        self._mapObj2Num[int(vNewGraphNode._obj._object_id)] = vNewGraphNode._num
        return vNewGraphNode

    def addLink( self, aGraphNode1, aGraphNode2 ):
        aGraphNode1._edges.add( aGraphNode2._num )

    def verNodesConsistency( self ):
        vVisited = set()
        for iGNd in self._nodes:
            if int(iGNd) in vVisited:
                print( "***ERROR: node added twice", file=sys.stderr )
                sys.quit()

    def nodeFromObj( self, aObjectId ):
        if int(aObjectId) in self._mapObj2Num:
            return self._nodes[self._mapObj2Num[int(aObjectId)]]
        else:
            return None

    def numFromObj( self, aObjectId ):
        return self._mapObj2Num[int(aObjectId)] if int(aObjectId) in self._mapObj2Num else None

    def tagAll( self, aTagVal ):
        for iNd in self._nodes:
            iNd._tag = aTagVal
        return self

    def nodesWithNoCaller( self ):
        vWithoutCallers = { x for x in range(len(self._nodes)) }
        
        for iNode in self._nodes:
            for iCalledNum in iNode._edges:
                if iCalledNum in vWithoutCallers:
                    vWithoutCallers.remove(iCalledNum)
        return vWithoutCallers

    def dumpGraph( self, aNodesDumpPath, aGraphDumpPath ):
        with open( aNodesDumpPath, "w") as vFile:
            print( "#0/A : Node num | 1/B: ObjectId | 2/C : ObjectName |3/D : ObjectFullname", file=vFile )
            for iNode in self._nodes:
                print( "|".join([
                    str(iNode._num), str(iNode._obj._object_id), str(iNode._obj._object_name), str(iNode._obj._object_fullname)
                ]), file=vFile )

        with open( aGraphDumpPath, "w") as vFile:
            print( "#0/A:CallerNum|1/B:CalleeNum|2/C:CallerObjectId|3/D:CalleeObjectId|4/E:CallerObjectName|5/F:CalleeObjectName|6/G:CallerObjectFullname|7/H:CalleeObjectFullname", file=vFile )
            for iNode in self._nodes:
                for iNum in iNode._edges:
                    vNode = self.node(iNum)
                    print( "|".join([
                        str(iNode._num), str(vNode._num), str(iNode._obj._object_id), str(vNode._obj._object_id),
                        iNode._obj._object_name, vNode._obj._object_name, iNode._obj._object_fullname, vNode._obj._object_fullname]),
                        file=vFile
                    )

    def _accessibleNodesRec( self, aNd ):
        retVal = tuple()

        if aNd._tag == -1:
            aNd._tag = 0 # open node
            retVal += ( aNd._num, )
            if not aNd.isLeaf():
                for iNum in aNd._edges:
                    vRes = self._accessibleNodesRec( self.node(iNum) )    
                    retVal += vRes
            aNd._tag = 1 # close node
                
        return retVal
        
    def accessibleNodes( self, aNodeFrom, vSet1=None, vSet2=None ):
        self.tagAll( -1 ) # all are free
        vRes = self._accessibleNodesRec(aNodeFrom)
        vRes1 = { x for x in vRes }
        vRes2 = { x._num for x in self._nodes if x._num not in vRes1 }

        vNbInSet1 = 0
        vNbInSet2 = 0
        for eNum in vRes1:
            if None!=vSet1 and eNum in vSet1:
                vNbInSet1 += 1
            if None!=vSet2 and eNum in vSet2:
                vNbInSet2 += 1
        return ( vRes1, vRes2, vNbInSet1, vNbInSet2 )

