import os
import sys
import json
import datetime
import xml.etree.ElementTree
import re
import collections

import logging
logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.DEBUG
)

C_X_CORRECTION = 1.0
C_Y_CORRECTION = 1.0

C_X_CORRECTION_POS = 1.2
C_Y_CORRECTION_POS = 1.3
C_X_CORRECTION_POS = 1.0
C_Y_CORRECTION_POS = 1.0

C_X_CORRECTION_VIS = 1.15
C_Y_CORRECTION_VIS = 1.25


TTransform = collections.namedtuple( "TTransform", "scX scY rot trX trY")
TRectangle = collections.namedtuple( "TRectangle", "x y w h")
TEdge = collections.namedtuple( "TEdge", [ "srce", "dest"])

class CTransform:
    __slots__ = "_scaleX", "_scaleY", "_rotate", "_translateX", "_translateY"

    def __init__( self, aScX, aScY, aRot, aTrX, aTrY ):
        self._scaleX = aScX
        self._scaleY = aScY
        self._rotate = aRot
        self._translateX = aTrX
        self._translateY = aTrY

    def apply( self, aX, aY ):
        return aX+self._translateX, aY+self._translateY

C_RX_GRAPH_TRANSFORM=re.compile( r'scale\(([0-9\.+-]+) ([0-9\.+-]+)\) rotate\(([0-9\.+-]+)\) translate\(([0-9\.+-]+) ([0-9\.+-]+)\)' )

class PosAuthor:
    def __init__( self, aOutputPath, aXCorrection=1.0, aYCorrection=1.0 ):
        self._path = aOutputPath
        self._corrX = aXCorrection
        self._corrY = aYCorrection

    def outputRectangle( self, aObjectId, aRect ):
        print( "rect|{}|{}|{}|{}|{}||".format(aObjectId,
            int(self._corrX*aRect.x), int(self._corrY*aRect.y), int(self._corrX*aRect.w), int(self._corrY*aRect.h)), file=self._file )

    def outputText( self, aObjectId, aText, aRect ):
        print( "text|{}|{}|{}|{}|{}|{}|".format(aObjectId,
            #int(self._corrX*aRect.x), int(self._corrY*aRect.y), int(self._corrX*aRect.w), int(self._corrY*aRect.h),aText), file=self._file )
            int(self._corrX*aRect.x), int(self._corrY*aRect.y), int(self._corrX*aRect.w), 80,aText), file=self._file )
    
    def outputTexts( self, aObjectId, aTexts, aRect ):
        print( "texts|{}|{}|{}|{}|{}|{}|".format(aObjectId,
            #int(self._corrX*aRect.x), int(self._corrY*aRect.y), int(self._corrX*aRect.w), int(self._corrY*aRect.h),aText), file=self._file )
            int(self._corrX*aRect.x), int(self._corrY*aRect.y), int(self._corrX*aRect.w), 80,"\\n".join(aTexts)), file=self._file )

    def outputObject( self, aObjectId, aRect ):
        print( "{}|{}|{}|{}|{}|{}||".format(aObjectId,aObjectId,
            int(self._corrX*aRect.x), int(self._corrY*aRect.y), int(self._corrX*aRect.w), int(self._corrY*aRect.h)), file=self._file )

    def _outputHeader( self ):
        print( "# 0/A:ObjectId | 1/B: ObjectName | 2/C: x | 3/D: y | 4/E: w | 5/F: h | 6/G: text | 7/H: ObjectFullname", file=self._file )

    def __enter__( self ):
        self._file = open( self._path, "w" )
        self._outputHeader()
        return self

    def __exit__( self, exc_type, exc_value, tb ):
        self._file.close()






def svg2Vis( aRectangle ):
    retValX = aRectangle.x + aRectangle.w/2
    retValY = aRectangle.y + aRectangle.h/2
    return (retValX,retValY)

def parsePolygonRectangle( aPoints, aTransform ):
    vMinX = min( x[0] for x in aPoints )
    vMaxX = max( x[0] for x in aPoints )
    vMinY = min( x[1] for x in aPoints )
    vMaxY = max( x[1] for x in aPoints )
    retVal = TRectangle(
        vMinX+aTransform.trX, vMinY+aTransform.trY,
        abs( ( vMaxX+aTransform.trX ) - ( vMinX+aTransform.trX ) ),
        abs( ( vMaxY+aTransform.trY ) - ( vMinY+aTransform.trY ) )
    )
    return retVal


def parsePolygonPoints( aPoints, aTransform ):
    retVal = None
    vPoints = [ ( float(x.split(",")[0]),float(x.split(",")[1]) ) for x in aPoints.split(" ") ]
    #logger.info( "    -> points: {}".format(vPoints) )
    if 5 == len(vPoints):
        retVal = parsePolygonRectangle( vPoints, aTransform )
    else:
        assert( false )
    return retVal

C_KIND_ENTRY_POINT  = "entry-point"
C_KIND_END_POINT    = "end-point"
C_KIND_NODE         = "regular-node"
C_WITH_CV           = "node-with-cv"
C_WITH_V            = "node-with-v"

TNodeJsData = collections.namedtuple( "TNodeJsData", [ "id", "x", "y", "label", "kind", "withCV", "withV" ] )

class VisnetworkAuthor:
    def __init__( self, aOutputPath, aXCorrection=1.0, aYCorrection=1.0 ):
        self._path = aOutputPath
        self._corrX = aXCorrection
        self._corrY = aYCorrection

    def outputJsDataProlog( self, aName ):
        print( "var {}=[".format(aName), file=self._file )

    def outputJsDataNode( self, aData ):
        #logger.info( "  output node: {}".format(aData) )
        vOutput = '  {{ id:{}, x:{}, y:{}, label:"{}", kind:"{}", withCV:{}, withV:{} }} ,'.format(
            aData.id, self._corrX*aData.x, self._corrY*aData.y, aData.label, aData.kind,"true" if aData.withCV else "false", "true" if aData.withV else "false")
        print( vOutput, file=self._file )

    def outputJsDataEpilog( self ):
        print( "]\n", file=self._file )


    def outputJsDataEdgeProlog( self, aName ):
        print( "var {}=[".format(aName), file=self._file )

    def outputJsDataEdge( self, aData ):
        #logger.info( "  output node: {}".format(aData) )
        vOutput = '  {{ from:{}, to:{} }} ,'.format(
            aData.srce, aData.dest)
        print( vOutput, file=self._file )

    def outputJsDataEdgeEpilog( self ):
        print( "]\n", file=self._file )

    def __enter__( self ):
        self._file = open( self._path, "w" )
        return self

    def __exit__( self, exc_type, exc_value, tb ):
        self._file.close()

C_RX_EDGE=re.compile(r'([0-9]+)->([0-9]+)')




class SvgParser:
    def __init__( self, aPath, aPosAuthor, aVisAuthor ):
        self._path = aPath
        self._xmlns = { "svg" : "http://www.w3.org/2000/svg"}
        self._pos = aPosAuthor
        self._vis = aVisAuthor

    def process( self ):
        with open( self._path, "r" ) as vFSvg:
            vSvg = self._process(vFSvg)

    def _process( self, aInFd ):
        vSvg =  xml.etree.ElementTree.parse(aInFd)

        vRoot = vSvg.getroot()

        # Get engloginb rectangle
        vWidth = vSvg.getroot().attrib["width"]
        vHeight = vSvg.getroot().attrib["height"]
        #logger.info( "  Area: {}x{}".format(vWidth,vHeight))

        vSvg = vRoot[0]
        self._vis.outputJsDataProlog( "G_CAST_OBJECTS" )

        if "class" in vSvg.attrib and "graph"==vSvg.attrib["class"] and "transform" in vSvg.attrib:
            vTransform = vSvg.attrib["transform"]
            vRes = C_RX_GRAPH_TRANSFORM.search(vTransform)
            if vRes:
                vTransform = TTransform(float(vRes[1]),float(vRes[2]),float(vRes[3]),float(vRes[4]),float(vRes[5]))
                #logger.info( "  Transform: {}".format(vTransform) )
            else:
                logger.error( "***ERROR: could not parse transform attribute: {}".format(vTransform) )
                assert(False)
            vPolygon = vSvg.find("svg:polygon",self._xmlns)
            vRect = parsePolygonPoints( vPolygon.attrib["points"], vTransform)
            #logger.info( "  Polygon: {} -> {}".format(vPolygon.attrib["points"],vRect) )
            self._pos.outputRectangle( "boxing", vRect )

        else:
            assert(False)

        # Explore all <g> xml nodes and links
        vEdges = []
        for iXmlNd in vSvg.findall("svg:g",self._xmlns):
            if "class" in iXmlNd.attrib and "node"==iXmlNd.attrib["class"]:
                self.parseGraphNode( self._xmlns, iXmlNd, vTransform )
            
            elif "class" in iXmlNd.attrib and "edge"==iXmlNd.attrib["class"]:
                self.parseGraphEdge( self._xmlns, iXmlNd, vTransform, vEdges )
        
        self._vis.outputJsDataEpilog()
        
        self._vis.outputJsDataEdgeProlog( "G_CAST_LINKS" )
        for iE in vEdges:
            self._vis.outputJsDataEdge( iE )
        self._vis.outputJsDataEdgeEpilog()

    def parseGraphNode( self, aXmlNs, aXmlNd, aTransform ):
        #logger.info( "  -- XmlNode: {}".format(aXmlNd) )
        # get CAST object ID
        vTitle = aXmlNd.find("svg:title",aXmlNs)
        vObjectId = vTitle.text
        #logger.info( "    -> Object-id : {}".format(vObjectId) )

        vPolygon = aXmlNd.find("svg:polygon",aXmlNs)
        vRect = parsePolygonPoints( vPolygon.attrib["points"], aTransform)
        #logger.info( "    -> Polygon   : {} -> {}".format(vPolygon.attrib["points"],vRect) )
        #outputRectangle( aOutFd, vObjectId, vRect )
        self._pos.outputObject( vObjectId, vRect )
        # Is Entry point or Exit point ? ie if stroke attribute == #ff4500
        vObjectKind = C_KIND_NODE
        vWithCViolation = False
        vWithViolation = False
        vStroke = vPolygon.attrib["stroke"].lower()
        vFill = vPolygon.attrib["fill"].lower()
        # Color code:
        #   Entry-point: fill: lightyellow or #ffffe0
        #   End-point: fill: cornsilk or #fff8dc
        #   w/ crit violation: stroke: red or #ff0000
        #   w/ non crit violation: stroke: lightsalmon or #ffffe0
        #   regular node: stroke and fill: white or #ffffff
        if "#ffffe0"==vFill or "lightyellow"==vFill:
            #logger.info( "    -> entry point" )
            vObjectKind = C_KIND_ENTRY_POINT

        elif "#fff8dc"==vFill or "cornsilk"==vFill:
            #logger.info( "    -> end point" )
            vObjectKind = C_KIND_END_POINT

        if vStroke in ( "#ff0000", "red" ):
            #logger.info( "    >> object with critical violations" )
            vWithCViolation = True

        elif vStroke in( "#ffa07a", "lightsalmon", "#4d4d4d" ):
            #logger.info( "    >> object with violations" )
            vWithViolation = True

        # Is object with critical violations ? ie if stroke attribute == black
        # Is object with violations ? ie if contains 
        vCriticalViolations = []
        vViolations = []
        vWithCriticalViolations = False
        vWithNonCriticalViolations = False
        vPrevX = vRect.x
        vPrevY = vRect.y
        vStack = []
        vObjectName = None
        vObjectType = None
        vObjectViolations = []
        for iSvgText in aXmlNd.findall("svg:text",aXmlNs):
            vFill = iSvgText.attrib["fill"]
            vX = float(iSvgText.attrib["x"])*C_X_CORRECTION+aTransform.trX
            vY = float(iSvgText.attrib["y"])*C_Y_CORRECTION+aTransform.trY
            if "#cd0000" == vFill:
                #logger.info( "    -> Crit. violation: {}".format(iSvgText.text) )
                vStack.append( ( iSvgText.text.strip(), TRectangle(vRect.x+6,vPrevY,vRect.w-6,vY-vPrevY ) ) )
                vWithCriticalViolations = True
                vObjectViolations.append( "<b><i>{}</i></b>".format(iSvgText.text.strip().replace('"','\\"')) )

            elif "#0000cd" == vFill:
                vObjectType = iSvgText.text
                #logger.info( "    -> Object type : {}".format(vObjectType) )
            
            elif "#cd00cd" == vFill:
                vObjectName = iSvgText.text
                #logger.info( "    -> Object name : {}".format(vObjectName) )

            elif "#666666" == iSvgText.attrib["fill"]:
                #logger.info( "    -> Non critical violation: {}".format(iSvgText.text) )
                vStack.append( ( iSvgText.text.strip(), TRectangle(vRect.x+6,vPrevY,vRect.w-6,vY-vPrevY ) ) )
                vWithNonCriticalViolations = True
                vObjectViolations.append( iSvgText.text.strip().replace('"','\\"') )

            vPrevX = vX
            vPrevY = vY

        if None!=vObjectId and None!=vObjectType and None!=vObjectName:
            vXV, vYV = svg2Vis( vRect )
            self._vis.outputJsDataNode(
                TNodeJsData( vObjectId, vXV, vYV, "<b>{}</b>\\n<i>{}</i>\\n{}".format(vObjectType,vObjectName,"\\n".join(vObjectViolations)), vObjectKind,
                    vWithCViolation, vWithViolation )
            )
            vObjectType, vObjectName = ( None, None )

        # Append stack reverse order: so it looks good in enlighten
        for i in reversed(range(len(vStack))) : # probably better to do that !!!
            self._pos.outputText( vObjectId, vStack[i][0], vStack[i][1] )
        if len(vStack)>0:
            self._pos.outputTexts( vObjectId, [ x[0] for x in vStack ], vStack[0][1] )
        #if vWithCriticalViolations or vWithNonCriticalViolations:
        if vWithCViolation or vWithViolation:
            self._pos.outputRectangle( vObjectId, TRectangle(vRect.x,vRect.y,vRect.w,vRect.h+8) )
        if vWithCViolation:
            self._pos.outputRectangle( vObjectId, TRectangle(vRect.x-2,vRect.y-2,vRect.w+4,vRect.h+12) )

    def parseGraphEdge( self, aXmlNs, aXmlNd, aTransform, aEdges  ):
        # Graph link generated by graphiviz & kyt:
        # title containns object ids
            #logger.info( "  -- XmlEdge: {}".format(aXmlNd) )
            vXmlTitle = aXmlNd.find("svg:title",aXmlNs)
            #print("  {}".format(vXmlTitle.text) )
            vRes = C_RX_EDGE.match(vXmlTitle.text)
            if vRes:
                #logger.info( "  -> edge: {} -> {}".format(vRes[1],vRes[2]) )
                aEdges.append( TEdge(vRes[1],vRes[2] ) )



def svgToVis( aInSvgPath, aOutPosPath, aOutJsDataPath ):
    with VisnetworkAuthor(aOutJsDataPath,C_X_CORRECTION_VIS, C_Y_CORRECTION_VIS) as vVis:
        with PosAuthor(aOutPosPath,C_X_CORRECTION_POS,C_Y_CORRECTION_POS) as vPos:
            vSvgP = SvgParser(aInSvgPath,vPos,vVis)
            vSvgP.process()



if __name__ == "__main__":
    logger.info( "Starting..." )
    Svg2Vis( sys.argv[1], sys.argv[2], sys.argv[3] )
    logger.info( "Finished." )