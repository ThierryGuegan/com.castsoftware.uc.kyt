import sys
import os

#import model.n_data
from model import n_data

#import common.n_graph
from common import n_graph

# File format
#0/A:TransactionId|1/B:TransactionName|2/C:TransactionType|3/D:TransactionFullname|4/E:ObjectId|5/F:ObjectName|6/G:ObjectType|7/H:ObjectFullname
def loadObjects( aFilePath ):
    retVal = {}
    #print( "Loading transaction objects from ["+aFilePath+"]...", file=sys.stderr )
    with open( aFilePath, "r" ) as vF:
        vLineNo = 0
        for vLine in vF:
            vLineNo += 1
            if '#' != vLine[0]:
                vFields = vLine.strip().split('|')
                retVal[int(vFields[4])] = n_data.CastObjectDesc( vFields[4], vFields[5], vFields[7], vFields[6] )
        print("  read "+str(vLineNo)+" lines.", file=sys.stderr )
    return retVal

# File format:
#0/A:TransactionId|1/B:Object1Id|2/C:Object2Id|3/D:TransactionName|4/E:TransactionFullname|5/F:Object1Name|6/G:Object1Fullname|7/H:Object2Name|8/I:Object2Fullname|9/J:LinkTypeName|10/K:LinkTypeDesc
def loadGraph( aFilePath, aCastObjectDescs ):
    retVal = n_graph.Graph()

    for iO in aCastObjectDescs.values():
    #    retVal.addNode( n_data.CastObject(iO._object_id) )
        retVal.addNode(iO )

    vNbIgnoredLinks = 0
    #print( "Loading transaction edges from ["+aFilePath+"]...", file=sys.stderr )
    with open( aFilePath, "r" ) as vF:
        vLineNo = 0
        vNode1 = None
        vNode2 = None
        for vLine in vF:
            vLineNo += 1
            if '#' != vLine[0] and vLine.strip():
                vFields = vLine.strip().split('|')
                vNode1Obj = vFields[1] 
                vNode2Obj = vFields[2]
                vNode1 = retVal.nodeFromObj( vNode1Obj )
                vNode2 = retVal.nodeFromObj( vNode2Obj )
                if vNode1!=None and vNode2!=None:
                    retVal.addLink( vNode1, vNode2 )
                else:
                    vNbIgnoredLinks += 1
                    if vNode1!=None or vNode2!=None:
                        print( "!!!WARNING: links with node not in graph: {0}: {1}".format(vLineNo,vLine.strip()) )
        print("  read {0} lines, ignored {1} links.".format(vLineNo,vNbIgnoredLinks), file=sys.stderr )

    return retVal

# File format
# 0/A:LocalObjectId|1/B:CentralObjectId|2/C:MetricId|3/D:isCritical|4/E:BCriterionId|5/F:TCriterionId|6/G:ObjectName|7/H:ObjectFullname|8/I:BCriterionName|9/J:TCriterionName|
#   10/K:MetricName|11/L:SnapshotId|12/M:ObjectDesc
def loadObjectsWithViolation( aFilePath, aGraph, aRetObjectDescs ):
    #print( "Loading transaction objects with critical violations from ["+aFilePath+"]...", file=sys.stderr )
    retVal = set()  # set of node number of objects with critical violation
    C_FIELD_OBJECT_ID = 0
    vNbIgnoredNodes = 0
    with open( aFilePath, "r" ) as vF:
        vLineNo = 0
        for vLine in vF:
            vLineNo += 1
            if '#' != vLine[0]:
                vFields = vLine.strip().split('|')
                vNode = aGraph.nodeFromObj(vFields[C_FIELD_OBJECT_ID])
                if None != vNode:
                    retVal.add( vNode._num )
                    #print( "Object with violations:",vNode._num)
                    if None != aRetObjectDescs:
                        if int(vNode._num) not in aRetObjectDescs:
                            aRetObjectDescs[int(vNode._num)] = []
                        aRetObjectDescs[int(vNode._num)].append(
                            n_data.CastObjectWithViolation(
                                vNode._obj, int(vFields[2]), vFields[10], vFields[3]
                            )
                        )
                else:
                    vNbIgnoredNodes += 1
        print("  read {0} lines, ignored {1} nodes".format(vLineNo,vNbIgnoredNodes), file=sys.stderr )
    return retVal

# File format
# 0/A:LocalObjectId|1/B:ObjectName|2/C:CentralObjectId|3/D:MetricId|4/E:MetricName|5/F:BCriterionId|6/G:BCriterionName|7/H:TCriterionId|8/I:TCriterionName|9/J:TWeight|
#  10/K:MWeight|11/L:TCrit|12/M:MCrit|13/N:TransactionId|14/O:TransactionName|15/P:ObjectFullname|16/Q:TransactionFullname"
def loadObjectsWithViolations( aFilePath, aGraph, aRetObjectDescs, aHealthFactors=(n_data.HF.ALL) ):
    #print( "Loading transaction objects with violations from ["+aFilePath+"]...", file=sys.stderr )
    retObjectNums = set()
    C_FIELD_OBJECT_ID = 0
    C_FIELD_OBJECT_NAME = 1
    C_FIELD_CENTRAL_OBJECT_ID = 2
    C_FIELD_METRIC_ID = 3
    C_FIELD_METRIC_NAME = 4
    C_FIELD_B_CRITERION_ID = 5
    C_FIELD_B_CRITERION_NAME = 6
    C_FIELD_T_CRITERION_ID = 7
    C_FIELD_T_CRITERION_NAME = 8
    C_FIELD_T_WEIGHT = 9
    C_FIELD_M_WEIGHT = 10
    C_FIELD_T_CRIT = 11
    C_FIELD_M_CRIT = 12
    C_FIELD_TRANS_ID = 13
    C_FIELD_TRANS_NAME = 14
    C_FIELD_OBJECT_FULLNAME = 15
    C_FIELD_TRANS_FULLNAME = 16
    vNbIgnoredNodes = 0
    with open( aFilePath, "r" ) as vF:
        vLineNo = 0
        vObjectNums = set()
        vRulesForObject = {}
        vPrevious = ( None, None )  # ( 0: vNode, 1: vFields ) for previous object
        for vLine in vF:
            vLineNo += 1
            if '#' != vLine[0]:
                vFields = vLine.strip().split('|')
                vNode = aGraph.nodeFromObj(vFields[C_FIELD_OBJECT_ID])
                if False and vPrevious[0] != vNode:
                    # New node
                    if None != vPrevious[0]:
                        vPrevNode = vPrevious[0]
                        vPrevFields = vPrevious[1]
                        # If previous node has non relevant violations it has not been outputed, so do it now
                        print( ">>>>>>>>> adding: {}: {}/{}: {}: {}".format(vIsCritical,int(float(vFields[C_FIELD_T_WEIGHT])),int(float(vFields[C_FIELD_M_WEIGHT])),vLineNo,vFields) )
                        aRetObjectDescs[int(vPrevNode._num)].append(
                            n_data.CastObjectWithViolation(
                                vPrevNode._obj, int(vPrevFields[C_FIELD_METRIC_ID]),
                                vPrevFields[C_FIELD_METRIC_NAME], False,
                                vPrevFields[C_FIELD_B_CRITERION_ID],
                                vPrevFields[C_FIELD_T_CRITERION_ID],
                                int(float(vPrevFields[C_FIELD_T_WEIGHT])), int(float(vPrevFields[C_FIELD_M_WEIGHT]))
                            )
                        )
                    else:
                        # first object => nothing to do, vPrevious will be updated
                        pass
                    vPrevious = ( vNode, vFields )

                if None != vNode:
                    retObjectNums.add( vNode._num )
                    #print( "Object with violations:",vNode._num)
                    if None != aRetObjectDescs and ( aHealthFactors==n_data.HF.ALL or int(vFields[C_FIELD_B_CRITERION_ID]) in aHealthFactors ):
                        if int(vNode._num) not in aRetObjectDescs:
                            aRetObjectDescs[int(vNode._num)] = []
                        #print( "read: {}: {}".format(vLineNo,vFields))
                        vIsCritical = False
                        if 1 == int(vFields[C_FIELD_M_CRIT]):
                            vIsCritical = True
                            if int(vNode._num) not in vRulesForObject:
                                vRulesForObject[int(vNode._num)] = []
                            vRulesForObject[int(vNode._num)].append( int(vFields[C_FIELD_METRIC_ID]) )

                        elif int(vFields[C_FIELD_B_CRITERION_ID]) in (60013,60014,60016):
                            if int(float(vFields[C_FIELD_T_WEIGHT]))>=7 and int(float(vFields[C_FIELD_M_WEIGHT]))>=6:
                                if int(vNode._num) not in vRulesForObject:
                                    vRulesForObject[int(vNode._num)] = [int(vFields[C_FIELD_METRIC_ID])]
                                else:
                                    vRulesForObject[int(vNode._num)].append( [int(vFields[C_FIELD_METRIC_ID])] )
                                #print( ">>>>>>>>> non critical: {}/{}: {}: {}".format(int(float(vFields[C_FIELD_T_WEIGHT])),int(float(vFields[C_FIELD_M_WEIGHT])),vLineNo,vFields) )
                            else:
                                continue
                        else:
                            # ignore rules for changeability and transferability
                            continue

                        #print( ">>>>>>>>> adding: {}: {}/{}: {}: {}".format(vIsCritical,int(float(vFields[C_FIELD_T_WEIGHT])),int(float(vFields[C_FIELD_M_WEIGHT])),vLineNo,vFields) )
                        aRetObjectDescs[int(vNode._num)].append(
                            n_data.CastObjectWithViolation(
                                vNode._obj, int(vFields[C_FIELD_METRIC_ID]),
                                vFields[C_FIELD_METRIC_NAME], vIsCritical,
                                vFields[C_FIELD_B_CRITERION_ID],
                                vFields[C_FIELD_T_CRITERION_ID],
                                int(float(vFields[C_FIELD_T_WEIGHT])), int(float(vFields[C_FIELD_M_WEIGHT]))
                            )
                        )
                else:
                    vNbIgnoredNodes += 1
        print("  read {0} lines, ignored {1} nodes".format(vLineNo,vNbIgnoredNodes), file=sys.stderr )
    return retObjectNums


# File format
# returns set of CAST object ids
#0/A:TransactionId|1/B:TransactionName|2/C:EndpointId|3/D:EndpointName|4/E:EndpointType[5/F:EndpointFullname|6/F:TransactionType|7/G:transactionFullname"
def loadTransactionEndpoints( aFilePath ):
    print( "[INFO ] Loading transaction endpoints from ["+aFilePath+"]...", file=sys.stderr )
    retVal = set()
    C_FIELD_OBJECT_ID = 2
    with open( aFilePath, "r" ) as vF:
        vLineNo = 0
        for vLine in vF:
            vLineNo += 1
            if '#' != vLine[0]:
                vFields = vLine.strip().split('|')
                retVal.add( int(vFields[C_FIELD_OBJECT_ID]) )
        print("[INFO ]   read {0} lines.", vLineNo, file=sys.stderr )
    return retVal
