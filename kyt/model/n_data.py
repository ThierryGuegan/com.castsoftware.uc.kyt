class HF:
    TQI             = 60017
    ROBUSTNESS      = 60013
    EFFICIENCY      = 60014
    SECURITY        = 60016
    CHANGEABILITY   = 60012
    TRANSFERABILITY = 60011
    ALL             = 99999

class CastObject:
    __slots__ = '_object_id'

    def __init__( self, aObjectId ):
        self._object_id = aObjectId

class CastObjectDesc:
    __slots__ = '_object_id', '_object_name', '_object_fullname', '_object_type'

    def __init__( self, aObjectId, aObjectName, aObjectFullname, aObjectType ):
        self._object_id = aObjectId
        self._object_name = aObjectName
        self._object_fullname = aObjectFullname
        self._object_type = aObjectType

    def objectName( self ):
        return self._object_name

class CastObjectWithViolation:
    __slots__ = '_object', '_metric_id', '_metric_name', '_is_critical', '_b_criterion', '_t_criterion', '_t_weight', '_m_weight'

    def __init__( self, aObject, aMetricId, aMetricName, aIsCritical, aBCriterion=HF.TQI, aTCriterion=None, aTWeight=0, aMWeight=0 ):
        self._object = aObject
        self._metric_id = aMetricId
        self._metric_name = aMetricName
        self._is_critical = aIsCritical
        self._b_criterion = aBCriterion
        self._t_criterion = aTCriterion
        self._t_weight = aTWeight
        self._m_weight = aMWeight

class CastTransaction:
    __slots__ = '_id', '_name', '_root', '_violations', '_endpoints'
    def __init__( self, aId, aName, aRoot, aViolations, aEndPoints ):
        self._id = aId
        self._name = aName
        self._root = aRoot
        self._violations = aViolations
        self._endpoints = aEndPoints

    def violationsOf( self, aObjNum ):
        retVal = None
        if aObjNum in self._violations:
            retVal = self._violations[aObjNum]
        return retVal