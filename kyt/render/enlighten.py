import sys
import os
import json
import time
import datetime
import collections
import psycopg2
import traceback
import logging

logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.INFO
)

from common import timewatch
from common import config 



##== Enlighten view helper class --------------------------------------------
class CCastEnlightenView:
    __slots__ = '_curs', '_localSchema', '_modId'
    vQuery3="set search_path={2}; SELECT I_ModRoot({0}, 0, {1}, 256, 257)"
    vQuery4="set search_path={6}; INSERT INTO ModKey VALUES ( {0}, 0, {1}, 0, {2}, {3}, {4}, {5}, 0)"
    vQuery4b="set search_path={2}; SELECT ObjDsc_SetLine({0},22000,0,0,'{1}',4,0);"
    vQuery4cText="set search_path={0}; SELECT I_Keys(0, 'File', '', 'FRETXT', -1, 20090, 1, 287, NULL);"   # prop=1, objtyp=287 for text
    vQuery4cRect="set search_path={0}; SELECT I_Keys(0, 'File', '', 'FRETXT', -1, 20090, 4, 557, NULL);"   # prop=1, objtyp=557 for rectangle

    def __init__( self, aCurs, aLocalSchema,aModId ):
        self._curs = aCurs
        self._localSchema = aLocalSchema
        self._modId = aModId

    def set( self, aCurs=None, aLocalSchema=None, aModId=None ):
        self._curs = aCurs if None!=aCurs else self._curs
        self._localSchema = aLocalSchema if None!=aLocalSchema else self._localSchema
        self._modId = aModId if None!=aModId else self._modId

    def reset( self ):
        self._curs = None
        self._localSchema = None
        self._modId = None

    def drawObject( self, aObjectId, aX, aY, aW, aH ):
        vRes = self._curs.execute( CCastEnlightenView.vQuery4.format(self._modId,aObjectId,aX,aY,aW,aH,self._localSchema) )
        
    def drawRectangle( self, aX, aY, aW, aH ):
        vRectKey = callStoredProc( self._curs, CCastEnlightenView.vQuery4cRect.format(self._localSchema))
        vRes = self._curs.execute( CCastEnlightenView.vQuery4.format(self._modId,vRectKey,aX,aY,aW,aH,self._localSchema) )
        
    def drawText( self, aText, aX, aY, aW, aH ):
        vTextKey = callStoredProc( self._curs, CCastEnlightenView.vQuery4cText.format(self._localSchema))
        vRes = self._curs.execute( CCastEnlightenView.vQuery4.format(self._modId,vTextKey,aX,aY,aW,aH,self._localSchema) )
        vRes = self._curs.execute( CCastEnlightenView.vQuery4b.format(vTextKey,escapeStr(aText),self._localSchema ) )

    def doObjPro( self, aObjPro ):
        for iO in aObjPro:
            vRes = self._curs.execute( CCastEnlightenView.vQuery3.format(self._modId,iO,self._localSchema) )
            vRes = self._curs.fetchall()
            vResRet = 0+vRes[0][0]


class CCastEnlighten:
    __slots__ = '_curs', '_localSchema'

    _vQueryFldExist="SELECT F.idfld FROM {0}.fld F WHERE F.fldnam='{1}'"
    _vQueryFldCreate = "set search_path={1}; SELECT I_Fld( 0, '{0}', 5, 20070, 1, 214, 'Default Folder/{0}' );"

    _vQueryModExist="SELECT M.idmod FROM {0}.mod M WHERE M.idfld={1} and M.modnam='{2}'"
    _vQueryModCreate="set search_path={3}; SELECT I_Mod( 0, '', '{2}', 20070, 1, 214, {1}, 'Default Folder/{0}' )"
    _vQueryModCreate2="set search_path={2};UPDATE mod SET idusr = '{1}' WHERE idmod={0}"
    _vQueryModCreate3=(
        # for all queries: parameters: 0: viewId aka modid, returns integer
        "set search_path={1}; SELECT I_ModCom({0}, 512, 513, 0, 0, 0)",
        "set search_path={1}; SELECT I_ModVew({0}, 0, 256, 500, 500, 50, 50, 900)",
        "set search_path={1}; SELECT I_ModLay(0, 'Default', 9, {0}, 0)",
        "set search_path={1}; SELECT I_ModLay(1, 'Belongs To', 25, {0}, 0)",
        "set search_path={1}; SELECT I_ModLay(2, 'Escalated', 9, {0}, 0)",
        "set search_path={1}; SELECT I_ModDspRul({0}, 0, 23001)",
        "set search_path={1}; SELECT I_ModDfc({0}, 0, 7)"
    )

    def __init__( self, aCurs, aLocalSchema ):
        self._curs = aCurs
        self._localSchema = aLocalSchema

    def getEnlightenFolderId( self, aEFolderName, aCreateIfNotExist=True ):
        retVal = None

        vCurs = self._curs
        # Folder already exist ?
        vRes = vCurs.execute( CCastEnlighten._vQueryFldExist.format(self._localSchema, aEFolderName) )
        vRes = vCurs.fetchall()

        if vRes:
            retVal = 0+vRes[0][0]
            logger.info( "folder {0} already exists: id: {1}".format(aEFolderName,retVal) )

        elif aCreateIfNotExist:
            # Create folder
            vRes = vCurs.execute( CCastEnlighten._vQueryFldCreate.format(aEFolderName,self._localSchema) )
            vRes = vCurs.fetchall()
            retVal = 0+vRes[0][0]
            logger.info( "created folder {0}: id: {1}".format(aEFolderName,retVal) )

        return retVal


    def createValidView( self, aEFolderName, aEFolderId, aEViewName ):
        retVal = None

        vContinue = True
        vIndex = 1
        vEViewName = aEViewName
        while vContinue:
            #print( "[DEBUG] query -> {"+vQueryExist.format(aLocalSchema, aEFolderId, vEViewName)+"}", file=sys.stderr )
            vRes = self._curs.execute( CCastEnlighten._vQueryModExist.format(self._localSchema, aEFolderId, vEViewName) )
            vRes = self._curs.fetchall()
            if vRes:
                # View name already exist for that folder => generate name
                logger.info( "view {0} already exists, generating new name...".format(vEViewName) )
                vEViewName = "{0}({1:0>3})".format(aEViewName,vIndex)
                vIndex += 1

            else:
                logger.info( "creating view {0}...".format(vEViewName) )
                
                vRes = self._curs.execute( CCastEnlighten._vQueryModCreate.format(aEFolderName, aEFolderId, vEViewName, self._localSchema ) )
                vRes = self._curs.fetchall()
                vModId = 0+vRes[0][0]
                logger.info( "created mod {0}: id: {1}".format(vEViewName, vModId) )
                    # update mod usr
                vRes = self._curs.execute( CCastEnlighten._vQueryModCreate2.format(vModId,"OPE",self._localSchema) )
                    # some stuffs
                for iQuery in CCastEnlighten._vQueryModCreate3:
                    vRes = self._curs.execute( iQuery.format(vModId,self._localSchema) )
                    vRes = self._curs.fetchall()
                    vResRet = 0+vRes[0][0]            
                    
                vContinue = False
                retVal = vModId

        return CCastEnlightenView(self._curs,self._localSchema,retVal)



# id in pos 3
def getObjPro( aObjects, aObjProFilePath ):
    retVal = []
    with open( aObjProFilePath, "r" ) as vF:
        for iO in vF:
            if iO[0:1] != '#':
                retVal.append( int(iO.strip()) )
    return retVal

def escapeStr( aStr ):
    return aStr.replace("'","''");

def callStoredProc( aCurs, aQuery ):
    vRes = aCurs.execute( aQuery )
    vRes = aCurs.fetchall()
    return 0+vRes[0][0]

def formatText( aText, aMaxCharPerLine=45 ):
    retVal = ""

    vText = aText
    #print( "0: vText:",len(vText),vText)
    while len(vText) > aMaxCharPerLine:
        # split on a space or a symbol
        iC = aMaxCharPerLine-1
        while len(vText) > aMaxCharPerLine:
            #print( "1: iC:",iC,":",vText[iC],'(',len(vText),vText,')')
            # char to break after
            if vText[iC] in ( ' ', '&', '#', ')', ']', '}', '=', '@', '^', '+', '-', '*', '/', '\\', ',', ';', ':', '!', '_', '|', '~', '$', '%', '.', '?', '>'):
                retVal += vText[:iC+1] + '\n'
                vText = vText[iC+1:]
                iC = aMaxCharPerLine-1
                #print( "2a: retVal: '{0}', vText: '{1}', len: {2}".format(retVal,vText,len(vText)))

            # char to break before
            elif vText[iC] in ( '{', '(', '[', '"', "'" ):
                retVal += vText[:iC] + '\n'
                vText = vText[iC:]
                iC = aMaxCharPerLine-1
                print( "2b: retVal: '{0}', vText: '{1}', len: {2}".format(retVal,vText,len(vText)))

            else:
                iC -= 1

        # chars to break after
    
    retVal += vText
    return retVal


def generateEnlightenViews( aCnx, aLocalSchema, aObjects, aPaths, aPathes, aObjProFilePath, aPosOutputFile ):
    with psycopg2.connect(aCnx) as vConn:
        vCE = CCastEnlighten(vConn.cursor(),aLocalSchema)
        vFldId = vCE.getEnlightenFolderId( aEFolderName, True )
        logger.info( "using folder id:".format(vFldId) )

        vEV = vCE.createValidView( aEFolderName, vFldId, aEViewName )

        vEV.doObjPro( getObjPro(aObjects,aObjProFilePath) )
        generateEnlightenViewA( aObjects, aObjProFilePath, aPosOutputFile )
        generateEnlightenViewB( aPaths, aPathes, aObjProFilePath, aPosOutputFile )

# aObjects: set: object_id -> [ ( critical violations )]
def generateEnlightenViewA( aObjects, aObjProFilePath, aPosOutputFile, aMode=0 ):

    C_BOX_MARGIN = 6
    C_BOX_MARGIN2 = 9

    # populate objects
    with open( aPosOutputFile, "w") as vPosFile:
        print( "# 0/A:ObjectId | 1/B: x | 2/C: y | 3/D: w | 4/E: h | 5/F: text", file=vPosFile )
        vXOrg, vYOrg = 500, 500
        vX = vXOrg
        vY = vYOrg
        vW = 480
        vH = 80
        vRow = 0
        vCol = 0
        vNum = 0
        vDeltaX = 540
        vDeltaY = 270
        vColMax = 5 # 5 = 6 - 1
        for iO in aObjects.keys():
            vCol = divmod(vNum,6)[1]
            vRow = divmod(vNum,6)[0]
            if 1 == vRow%2:
                vCol = vColMax-vCol

            if 0 == aMode:
                vX = vXOrg + vCol*vDeltaX
                vY = vYOrg + vRow*vDeltaY
            elif 1 == aMode:
                vX = vXOrg + vCol*vDeltaX
                if 1 == vCol%2:
                    vY = vYOrg + (vRow+1)*vDeltaY-1-vH
                else:
                    vY = vYOrg + vRow*vDeltaY
            elif 2 == aMode:
                vX = vXOrg + vCol*vDeltaX
                if 0 == vRow%2:
                    vY = vYOrg + int( vRow*vDeltaY + vCol*(vDeltaY-vH)/6 )
                else:
                    vY = vYOrg + int( vRow*vDeltaY + (vColMax - vCol)*(vDeltaY-vH)/6 )
                
            #vRes = vCurs.execute( vQuery4.format(vModId,iO,vX,vY,vW,vH,aLocalSchema) )
            vEV.drawObject( iO, vX, vY, vW, vH )

            # 0/A:ObjectId | 1/B: ObjectName | 2/C: x | 3/D: y | 4/E: w | 5/F: h | 6/G: text | 7/H: ObjectFullname
            #old-version#print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format(iO,aObjects[iO][4],vX,vY,vW,vH,"",aObjects[iO][5]), file=vPosFile )
            print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format(
                iO, aObjects[iO][0][4],vX,vY,vW,vH,"",aObjects[iO][0][5]), file=vPosFile )
            vNum += 1
            
            vNbDefects = 0
            vNbNonCriticalDefects = 0
            vNbCriticalDefects = 0
            vTexts = []
            vTextsCritical = []
            for iDefect in aObjects[iO]:
                if '*' == iDefect[0] :
                    vNbCriticalDefects += 1
                    vTextsCritical.append( "*"+iDefect[7] )

                elif '-' == iDefect[0] :
                    vNbNonCriticalDefects += 1
                    vTexts.append( iDefect[7] )
            vNbDefects = vNbNonCriticalDefects + vNbCriticalDefects

            if vNbDefects > 0:
                if 0 == vNbNonCriticalDefects :
                    # Only critical violations, put 2 max
                    vText = "{} crit. defect(s):".format(vNbDefects )
                    vText += "\n  "+vTextsCritical[0]
                    if vNbCriticalDefects>1:
                        vText += "\n  "+vTextsCritical[1]

                elif 0 == vNbCriticalDefects :
                    # Only non-critical violations, put 2 max
                    vText = "{} non-crit. defect(s):".format(vNbDefects)
                    vText += "\n  "+vTexts[0]
                    if vNbDefects>1:
                        vText += "\n  "+vTexts[1]

                else:
                    # Both critical and non-critical violations
                    vText = "{} crit. defect(s), {} non-crit. defect(s):".format(vNbCriticalDefects,vNbDefects)
                    vText += "\n  "+vTextsCritical[0]
                    if vNbCriticalDefects>1:
                        vText += "\n  "+vTextsCritical[1]
                    elif vNbCriticalDefects > 0 :
                        vText += "\n  "+vTexts[0]


                vEV.drawText( vText, vX, vY+vH, vW, int(3*vH/2) )

                print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format("","text",vX,vY,vW,vH,vText,""), file=vPosFile )

                vEV.drawRectangle( vX-C_BOX_MARGIN,vY-C_BOX_MARGIN,vW+2*C_BOX_MARGIN,int(5*vH/2)+2*C_BOX_MARGIN )
                print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format("","rectangle",vX,vY,vW,vH,"",""), file=vPosFile )

                if vNbCriticalDefects>0:
                    vEV.drawRectangle( vX-C_BOX_MARGIN2,vY-C_BOX_MARGIN2,vW+2*C_BOX_MARGIN2,int(5*vH/2)+2*C_BOX_MARGIN2 )
                    print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format("","rectangle",vX,vY,vW,vH,"",""), file=vPosFile )



# aObjects: set: object_id -> [ ( critical violations )]
def generateEnlightenView( aCnx, aLocalSchema, aEFolderName, aEViewName, aObjects, aObjProFilePath, aPosOutputFile, aMode=0 ):
    vCnx = aCnx

    C_BOX_MARGIN = 6
    C_BOX_MARGIN2 = 9

    with psycopg2.connect(vCnx) as vConn:

        vCE = CCastEnlighten(vConn.cursor(),aLocalSchema)
        vFldId = vCE.getEnlightenFolderId( aEFolderName, True )
        logger.info( "using folder id:".format(vFldId) )
        
        #vModId = vCE.createValidView( aEFolderName, vFldId, aEViewName )
        #vEV = CCastEnlightenView( vCurs, aLocalSchema, vModId )
        vEV = vCE.createValidView( aEFolderName, vFldId, aEViewName )
        
        # Add objects to view
        # populate object browser ??
        vEV.doObjPro( getObjPro(aObjects,aObjProFilePath) )

        # populate objects
        with open( aPosOutputFile, "w") as vPosFile:
            print( "# 0/A:ObjectId | 1/B: x | 2/C: y | 3/D: w | 4/E: h | 5/F: text", file=vPosFile )
            vXOrg, vYOrg = 500, 500
            vX, vY = vXOrg, vYOrg
            vW, vH = 480, 80
            vRow, vCol, vNum = 0, 0, 0
            vDeltaX, vDeltaY = 540, 270
            vColMax = 5 # 5 = 6 - 1

            for iO in aObjects.keys():
                vRow, vCol = divmod(vNum,6)
                
                if 1 == vRow%2:
                    vCol = vColMax-vCol

                if 0 == aMode:
                    vX = vXOrg + vCol*vDeltaX
                    vY = vYOrg + vRow*vDeltaY
                elif 1 == aMode:
                    vX = vXOrg + vCol*vDeltaX
                    if 1 == vCol%2:
                        vY = vYOrg + (vRow+1)*vDeltaY-1-vH
                    else:
                        vY = vYOrg + vRow*vDeltaY
                elif 2 == aMode:
                    vX = vXOrg + vCol*vDeltaX
                    if 0 == vRow%2:
                        vY = vYOrg + int( vRow*vDeltaY + vCol*(vDeltaY-vH)/6 )
                    else:
                        vY = vYOrg + int( vRow*vDeltaY + (vColMax - vCol)*(vDeltaY-vH)/6 )
                    
                #vRes = vCurs.execute( vQuery4.format(vModId,iO,vX,vY,vW,vH,aLocalSchema) )
                vEV.drawObject( iO, vX, vY, vW, vH )

                # 0/A:ObjectId | 1/B: ObjectName | 2/C: x | 3/D: y | 4/E: w | 5/F: h | 6/G: text | 7/H: ObjectFullname
                #old-version#print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format(iO,aObjects[iO][4],vX,vY,vW,vH,"",aObjects[iO][5]), file=vPosFile )
                print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format(
                    iO, aObjects[iO][0][4],vX,vY,vW,vH,"",aObjects[iO][0][5]), file=vPosFile )
                vNum += 1
                
                vNbDefects = 0
                vNbNonCriticalDefects = 0
                vNbCriticalDefects = 0
                vTexts = []
                vTextsCritical = []
                for iDefect in aObjects[iO]:
                    if '*' == iDefect[0] :
                        vNbCriticalDefects += 1
                        vTextsCritical.append( "*"+iDefect[7] )

                    elif '-' == iDefect[0] :
                        vNbNonCriticalDefects += 1
                        vTexts.append( iDefect[7] )
                vNbDefects = vNbNonCriticalDefects + vNbCriticalDefects

                if vNbDefects > 0:
                    if 0 == vNbNonCriticalDefects :
                        # Only critical violations, put 2 max
                        vText = "{} crit. defect(s):".format(vNbDefects )
                        vText += "\n  "+vTextsCritical[0]
                        if vNbCriticalDefects>1:
                            vText += "\n  "+vTextsCritical[1]

                    elif 0 == vNbCriticalDefects :
                        # Only non-critical violations, put 2 max
                        vText = "{} non-crit. defect(s):".format(vNbDefects)
                        vText += "\n  "+vTexts[0]
                        if vNbDefects>1:
                            vText += "\n  "+vTexts[1]

                    else:
                        # Both critical and non-critical violations
                        vText = "{} crit. defect(s), {} non-crit. defect(s):".format(vNbCriticalDefects,vNbDefects)
                        vText += "\n  "+vTextsCritical[0]
                        if vNbCriticalDefects>1:
                            vText += "\n  "+vTextsCritical[1]
                        elif vNbCriticalDefects > 0 :
                            vText += "\n  "+vTexts[0]


                    vEV.drawText( vText, vX, vY+vH, vW, int(3*vH/2) )

                    print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format("","text",vX,vY,vW,vH,vText,""), file=vPosFile )

                    vEV.drawRectangle( vX-C_BOX_MARGIN,vY-C_BOX_MARGIN,vW+2*C_BOX_MARGIN,int(5*vH/2)+2*C_BOX_MARGIN )
                    print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format("","rectangle",vX,vY,vW,vH,"",""), file=vPosFile )

                    if vNbCriticalDefects>0:
                        vEV.drawRectangle( vX-C_BOX_MARGIN2,vY-C_BOX_MARGIN2,vW+2*C_BOX_MARGIN2,int(5*vH/2)+2*C_BOX_MARGIN2 )
                        print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format("","rectangle",vX,vY,vW,vH,"",""), file=vPosFile )




def generateEnlightenView2( aCnx, aLocalSchema, aEFolderName, aEViewName, aPaths, aPathes, aObjProFilePath, aPosOutputFile ):
    vCnx = aCnx

    logger.info( "-- generating view for [{0}/{1}] ...".format(aEFolderName,aEViewName) )

    C_BOX_MARGIN = 6
    C_BOX_MARGIN2 = 9

    with psycopg2.connect(vCnx) as vConn:
        
        vCE = CCastEnlighten(vConn.cursor(),aLocalSchema)
        vFldId = vCE.getEnlightenFolderId( aEFolderName, True )
        logger.info("  using folder id: {}".format(vFldId) )
        
        vEV = vCE.createValidView( aEFolderName, vFldId, aEViewName )
        vEV.doObjPro( getObjPro(None,aObjProFilePath) )

        # populate objects
        vXOrg = 500
        vYOrg = 500
        
        vDeltaX = 540
        vW = 480
        vH = 80
        vDeltaY = int(3*vH) # 240 instad of initial 270

        vX = vXOrg
        vDrawn = set()
        vFirst = True
        vNbMaxRules = 4

        # compute nb of real paths from root : number of distinct root children
        vRChildren = { x[1][7] for x in aPaths }            

        with open( aPosOutputFile, "w") as vPosFile:
            print( "# 0/A:ObjectId | 1/B: ObjectName | 2/C: x | 3/D: y | 4/E: w | 5/F: h | 6/G: text | 7/H: ObjectFullname", file=vPosFile )
            
            for iPath in aPathes:
                vY = vYOrg
                #for iO in iPath.keys():
                for iObjectId in iPath.keys():
                    vNbRules = 0
                    
                    if vFirst:
                        vX = int( ( vXOrg + len(vRChildren)*vDeltaX )/2 )
                        logger.info( "vXOrg: {}, len: {}  =>  vX={}".format(vXOrg,len(vRChildren),vX ) )

                    if iObjectId not in vDrawn:
                        vEV.drawObject( iObjectId, vX, vY, vW, vH )
                        
                        # 0/A:ObjectId | 1/B: x | 2/C: y | 3/D: w | 4/E: h | 5/F: text
                        print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format(
                            iObjectId,iPath[iObjectId][0][8],vX,vY,vW,vH,"",iPath[iObjectId][0][9]), file=vPosFile )

                        vNbCriticals = 0
                        vNbNonCriticals = 0
                        vNonCriticalRuleNames = []
                        vCriticalRuleNames = []

                        for iV in iPath[iObjectId]:
                            if '*' == iV[0]:
                                vNbCriticals +=1
                                vCriticalRuleNames.append( iV[11] )

                            elif '-' == iV[0]:
                                vNbNonCriticals += 1
                                vNonCriticalRuleNames.append( iV[11] )

                        vNbViolations = vNbCriticals + vNbNonCriticals
                        vText = "??????"
                        vHealthFactors = ""
                        if vNbViolations > 0:
                            if 0 == vNbNonCriticals:
                                # Only critical violations
                                if vNbViolations <= 1:
                                    vText = "{} {} crit. violation:".format(vNbViolations,vHealthFactors)
                                else:
                                    vText = "{} {} crit. violations:".format(vNbViolations,vHealthFactors)

                                while vNbRules<vNbViolations and vNbRules<vNbMaxRules:
                                    vText += "\n  *"+vCriticalRuleNames[vNbRules] #[:30]
                                    vNbRules += 1

                            elif 0 == vNbCriticals:
                                # Only non critical violations
                                if vNbViolations <= 1 :
                                    vText = "{} {} non-crit. violation:".format(vNbViolations,vHealthFactors)
                                else:
                                    vText = "{} {} non-crit. violations:".format(vNbViolations,vHealthFactors)

                                while vNbRules<vNbViolations and vNbRules<vNbMaxRules and len(vText)<255:
                                    vText += "\n  "+vNonCriticalRuleNames[vNbRules] #[:30]
                                    vNbRules += 1

                            else:
                                # Both non critical and critical violations
                                if vNbCriticals == 0:
                                        vText = "{} Non-crit. violations:".format(vHealthFactors)
                                elif vNbCriticals == 1 :
                                    vText = "{} {} crit. violation:".format(vNbCriticals,vHealthFactors)
                                else:
                                    vText = "{} {} crit. violations:".format(vNbCriticals,vHealthFactors)
                                
                                while vNbRules<vNbCriticals and vNbRules<vNbMaxRules  and len(vText)<255:
                                    vText += "\n  *"+vCriticalRuleNames[vNbRules] #[:30]
                                    vNbRules += 1

                                while (vNbRules-vNbCriticals)<vNbNonCriticals and vNbRules<vNbMaxRules  and len(vText)<255:
                                    vText += "\n  "+vNonCriticalRuleNames[vNbRules-vNbCriticals] #[:30]
                                    vNbRules += 1

                            if len(vText)>255:
                                vText = vText[:255]

                            vHText = vH
                            if 2 < vNbMaxRules:
                                vHText = int( (1+vNbRules)*vH/2)

                            # Total lines of text of text box: 1 + vNbRules
                            # Englobing box for text heigh is = NbLinesOfText*vH/2
                            vEV.drawText( vText, vX, vY+vH, vW, vHText )
                            print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format("","text", vX,vY,vW,vH,vText,""), file=vPosFile )

                            vEV.drawRectangle( vX-C_BOX_MARGIN,vY-C_BOX_MARGIN,vW+2*C_BOX_MARGIN,vH+vHText+2*C_BOX_MARGIN )
                            print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format("","rectangle",vX,vY,vW,vH,"",""), file=vPosFile )

                            if vNbCriticals>0:
                                vEV.drawRectangle( vX-C_BOX_MARGIN2,vY-C_BOX_MARGIN2,vW+2*C_BOX_MARGIN2,vH+vHText+2*C_BOX_MARGIN2 )
                                print( "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}".format("","rectangle",vX,vY,vW,vH,"",""), file=vPosFile )
                            if 2 != vNbMaxRules:
                                vY += vH +vHText + ( vDeltaY - 2*vH ) - vDeltaY


                        vDrawn.add( iObjectId )

                    if vFirst:
                        vX = vXOrg
                        vFirst = False
                    vY += vDeltaY

                vX += vDeltaX




def processTransaction( aCnx, aLocalSchema, aEFolderName, aEViewName, aSingleTransactionFilePath, aMultipleTransactionFilePath ):
    pass
    
def drawEnlightenViews( aClientAppRootFolder, aClientAppTrSubFolder, aEViewName, aCnx, aLocalSchema ):

    vEFolderName = "Tr_"+aEViewName

    ##==
    vClientAppSubFolderPath = "{0}\\{1}".format(aClientAppRootFolder,aClientAppTrSubFolder) # TODO: removed when next line validated
    vClientAppSubFolderPath = os.path.join( aClientAppRootFolder, aClientAppTrSubFolder, "_paths")

    vObjProFilePath = "{0}\\40_objects-objpro.txt".format(vClientAppSubFolderPath) # TODO: removed when next line validated
    vObjProFilePath = os.path.join( aClientAppRootFolder, aClientAppTrSubFolder, "_data", "40_objects-objpro.txt" )
    
    for iDataKind in ( '00', '01', '02', '03', '04' ):
        # one path files
        vObjectFilePath = os.path.join( vClientAppSubFolderPath, "99_objects-{0}.txt".format(iDataKind) )
        vOutputPosFile = os.path.join( vClientAppSubFolderPath, "enlighten-{0}.txt".format(iDataKind) )

        if os.path.exists( vObjectFilePath ):
            logger.info( "    using object file [{0}]".format(vObjectFilePath) )
            logger.info( "    using objpro file [{0}]".format(vObjProFilePath) )
            logger.info( "    generating position file [{0}]".format(vOutputPosFile) )
            
            # load object ids
            vObjects= {}
            vIndex = 0
            with open( vObjectFilePath, "r") as vF:
                for iLine in vF:
                    if iLine[0:1] != '#':
                        vFieldsT = tuple(iLine.strip().split('|'))
                        vIndex = int(vFieldsT[3])
                        if vIndex in vObjects:
                            vObjects[vIndex].append( vFieldsT )
                        else:
                            vObjects[vIndex] = [ vFieldsT ]
            generateEnlightenView( aCnx, aLocalSchema, vEFolderName, "T_{0}-{1}".format(aEViewName,iDataKind), vObjects, vObjProFilePath, vOutputPosFile, 0 )
        else:
            print( "File [{0}] does not exist, skipping to the next one.".format(vObjectFilePath), file=sys.stderr )

        # all paths file
        vObjectFilePath = os.path.join( vClientAppSubFolderPath, "99_objects-all-{0}.txt".format(iDataKind) )
        if os.path.exists( vObjectFilePath ):
            logger.info( "  using object file [{0}]".format(vObjectFilePath) )

            # load object ids
            vPaths = [] # list of paths: each path contains list of objects in that path
            vPathes = []
            vCurrPath = -1
            vObjects= None
            vObjectsOfPath = None
            vIndex = -1
            with open( vObjectFilePath, "r") as vF:
                #All#0:Critical|1:NbPaths|2:PathNum|3:PathLen|4:StepNum|5:TransactionId|6:TransactionName|7:ObjectId|8:ObjectName|9:ObjectFullname|10:MetricId|11:MetricName
                vLineNo = 0
                for iLine in vF:
                    vLineNo += 1
                    if iLine[0:1] != '#':
                        vFields = tuple(iLine.strip().split('|'))
                        vIndex = int(vFields[7])
                        vNbPaths = int(vFields[1])
                        vPathNum = int(vFields[2])
                        if vPathNum > vCurrPath:
                            # new path
                            vObjects = []
                            vObjectsOfPath = {}
                            vPaths.append( vObjects )
                            vPathes.append( vObjectsOfPath )
                            vCurrPath += 1

                        if vIndex not in vObjectsOfPath:
                            vObjectsOfPath[vIndex] = []
                        vObjectsOfPath[vIndex].append( vFields )
                        vObjects.append( vFields )

            if len(vPaths)>0 and len(vPaths[0]):
                generateEnlightenView2( aCnx, aLocalSchema, vEFolderName, "T_{0}-all-{1}".format(aEViewName,iDataKind), vPaths, vPathes, vObjProFilePath, vOutputPosFile )
            else:
                logger.warning( "no paths generated, skipping file." )

        else:
            print( "File [{0}] does not exist: cannot output 'all' view.".format(vObjectFilePath), file=sys.stderr )

#TODO: to be somewhere else cause copy/past and used in other scripts
def _postgresConnectionString( aDbConfig ):
    #return "host='"+aDbConfig['db-server']+"' port="+aDbConfig['db-port']+" dbname='"+aDbConfig['db-base']+"' user='"+aDbConfig['db-login']+"' password='"+aDbConfig['db-password']+"'"
    return "host='{0}' port={1} dbname='{2}' user='{3}' password='{4}'".format(
            aDbConfig['db-server'], aDbConfig['db-port'], aDbConfig['db-base'], aDbConfig['db-login'], aDbConfig['db-password']
        )


def renderTransactionInEnlighten( aOptions ):
    if 'enable-enlighten' in aOptions["transaction-config"] and aOptions["transaction-config"]["enable-enlighten"]:
        vOutputRootFolder = aOptions["output-root-folder"]
        vOutputTrRootFolder = aOptions["tr-output-folder"]
        vEViewName = aOptions["transaction-subfolder"]
        vCnxStr = _postgresConnectionString( aOptions['db-config'] )
        drawEnlightenViews( vOutputRootFolder, vOutputTrRootFolder, vEViewName, vCnxStr, aOptions['db-config']['db-local'] )
    else:
        logger.warning( "!Warning: skipping transaction [{}] cause enable-enlighten not set".format(aOptions['transaction']) )

def kytEnlightenMain( aArgv ):
    if 0==len(aArgv) or not os.path.isfile( aArgv[0] ):
        logger.error( "***ERROR: No valid configuration file, exiting." )
        return
    vConfiguration = config.CConfig(aArgv[0])
    vConfiguration.processConfigurations(renderTransactionInEnlighten,None, False)


if __name__ == "__main__":
    logger.info( "Starting...", +1 )
    youCastTransactionEnlighen(sys.argv[1:])
    logger.info( "Finished.", -1 )