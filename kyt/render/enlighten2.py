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
from output import filenames

def callStoredProc( aCurs, aQuery ):
    vRes = aCurs.execute( aQuery )
    vRes = aCurs.fetchall()
    return 0+vRes[0][0]

def getObjPro(aObjProFilePath ):
    retVal = []
    with open( aObjProFilePath, "r" ) as vF:
        for iO in vF:
            if iO[0:1] != '#':
                retVal.append( int(iO.strip()) )
    return retVal

def escapeStr( aStr ):
    return aStr.replace("'","''");

class CCastEnlightenView:
    __slots__ = '_curs', '_localSchema', '_modId', "_xFactor", "_yFactor"
    vQuery3="set search_path={2}; SELECT I_ModRoot({0}, 0, {1}, 256, 257)"
    vQuery4="set search_path={6}; INSERT INTO ModKey VALUES ( {0}, 0, {1}, 0, {2}, {3}, {4}, {5}, 0)"
    vQuery4b="set search_path={2}; SELECT ObjDsc_SetLine({0},22000,0,0,'{1}',4,0);"
    vQuery4cText="set search_path={0}; SELECT I_Keys(0, 'File', '', 'FRETXT', -1, 20090, 1, 287, NULL);"   # prop=1, objtyp=287 for text
    vQuery4cRect="set search_path={0}; SELECT I_Keys(0, 'File', '', 'FRETXT', -1, 20090, 4, 557, NULL);"   # prop=1, objtyp=557 for rectangle

    def __init__( self, aCurs, aLocalSchema, aModId, aXFactor=1.0, aYFactor=1.0 ):
        self._curs = aCurs
        self._localSchema = aLocalSchema
        self._modId = aModId
        self._xFactor = aXFactor
        self._yFactor = aYFactor

    def set( self, aCurs=None, aLocalSchema=None, aModId=None ):
        self._curs = aCurs if None!=aCurs else self._curs
        self._localSchema = aLocalSchema if None!=aLocalSchema else self._localSchema
        self._modId = aModId if None!=aModId else self._modId

    def reset( self ):
        self._curs = None
        self._localSchema = None
        self._modId = None

    def drawObject( self, aObjectId, aX, aY, aW, aH ):
        # IMPORTANT: object always have same height
        vRes = self._curs.execute( CCastEnlightenView.vQuery4.format(self._modId,aObjectId,
            int(self._xFactor*aX),int(self._yFactor*aY),
            int(self._xFactor*aW),80, #int(self._yFactor*aH),
            self._localSchema) )
        
    def drawRectangle( self, aX, aY, aW, aH ):
        vRectKey = callStoredProc( self._curs, CCastEnlightenView.vQuery4cRect.format(self._localSchema))
        vRes = self._curs.execute( CCastEnlightenView.vQuery4.format(
            self._modId, vRectKey,
            int(self._xFactor*aX), int(self._yFactor*aY),
            int(self._xFactor*aW), int(self._yFactor*aH),
            self._localSchema) )
        
    def drawText( self, aText, aX, aY, aW, aH ):
        vTextKey = callStoredProc( self._curs, CCastEnlightenView.vQuery4cText.format(self._localSchema))
        vRes = self._curs.execute( CCastEnlightenView.vQuery4.format(
            self._modId, vTextKey,
            int(self._xFactor*aX), int(self._yFactor*aY),
            int(self._xFactor*aW), int(self._yFactor*aH),
            self._localSchema) )
        vRes = self._curs.execute( CCastEnlightenView.vQuery4b.format(vTextKey,escapeStr(aText),self._localSchema ) )

    def drawTexts( self, aTexts, aX, aY, aW, aH ):
        vLines = aTexts.split("\\n")
        vY = int(self._yFactor*aY) + 20 + (len(vLines)-1)*40
        for iN in range(0,len(vLines)):
            iL = vLines[len(vLines)-1-iN]
            vTextKey = callStoredProc( self._curs, CCastEnlightenView.vQuery4cText.format(self._localSchema))
            vRes = self._curs.execute( CCastEnlightenView.vQuery4.format(
                self._modId, vTextKey,
                int(self._xFactor*aX), vY, # int(self._yFactor*vY),
                int(self._xFactor*aW), 40, # 3*80/2, #int(self._yFactor*aH),
                self._localSchema) )
            vRes = self._curs.execute( CCastEnlightenView.vQuery4b.format(vTextKey,escapeStr(iL),self._localSchema ) )
            vY -= 40

    def doObjPro( self, aObjPro ):
        for iO in aObjPro:
            vRes = self._curs.execute( CCastEnlightenView.vQuery3.format(self._modId,iO,self._localSchema) )
            vRes = self._curs.fetchall()
            vResRet = 0+vRes[0][0]


class CCastEnlighten:
    #__slots__ = '_curs', '_localSchema'

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

    def __init__( self, aCnx, aLocalSchema ):
        self._curs = None
        self._cnx = aCnx
        self._localSchema = aLocalSchema
        self._conn = None 

    def __enter__( self ):
        self._conn = psycopg2.connect(self._cnx)
        self._curs = self._conn.cursor()
        return self

    def __exit__( self, exc_type, exc_value, tb ):
        logger.info( "------ exiting ------")
        if self._curs: self._curs.close()
        if self._conn: self._conn.close()

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
            logger.info( "  QUERY: {}".format(CCastEnlighten._vQueryFldCreate.format(aEFolderName,self._localSchema)) )

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
                logger.info( "  view {0} already exists, generating new name...".format(vEViewName) )
                vEViewName = "{0}({1:0>3})".format(aEViewName,vIndex)
                vIndex += 1

            else:
                logger.info( "  creating view {0}...".format(vEViewName) )
                
                vRes = self._curs.execute( CCastEnlighten._vQueryModCreate.format(aEFolderName, aEFolderId, vEViewName, self._localSchema ) )
                vRes = self._curs.fetchall()
                vModId = 0+vRes[0][0]
                logger.info( "    created mod {0}: id: {1}".format(vEViewName, vModId) )
                    # update mod usr
                vRes = self._curs.execute( CCastEnlighten._vQueryModCreate2.format(vModId,"OPE",self._localSchema) )
                    # some stuffs
                for iQuery in CCastEnlighten._vQueryModCreate3:
                    vRes = self._curs.execute( iQuery.format(vModId,self._localSchema) )
                    vRes = self._curs.fetchall()
                    vResRet = 0+vRes[0][0]            
                    
                vContinue = False
                retVal = vModId

        return CCastEnlightenView(self._curs,self._localSchema,retVal,1.3,3)

class Context:
    def __init__(self):
        self._xorg = 0
        self._yorg = 0
        self._width = 0
        self._height = 0

def parsePosLine( aContext, aLineNo, aLine, aEView ):
    #logger.info( "  parsing line {:<3}: {{{}}}".format(aLineNo,aLine) )
    vFields = aLine.split('|')

    if "rect"==vFields[0] and "boxing"==vFields[1]:
        aContext._xorg = int(vFields[2])
        aContext._yorg = int(vFields[3])
        aContext._width = int(vFields[4])
        aContext._height = int(vFields[5])
        aEView.drawRectangle( 0, 0, aContext._width, aContext._height )

    elif "rect" == vFields[0]:
        vObjectId = int(vFields[1])
        vX = int(vFields[2])
        vY = int(vFields[3])
        vW = int(vFields[4])
        vH = int(vFields[5])
        aEView.drawRectangle( vX-aContext._xorg, vY-aContext._yorg, vW, vH )

    elif "text" == vFields[0]:
        vObjectId = int(vFields[1])
        vX = int(vFields[2])
        vY = int(vFields[3])
        vW = int(vFields[4])
        vH = int(vFields[5])
        #aEView.drawText( vFields[6], vX-aContext._xorg, vY-aContext._yorg, vW, vH )

    elif "texts" == vFields[0]:
        vObjectId = int(vFields[1])
        vX = int(vFields[2])
        vY = int(vFields[3])
        vW = int(vFields[4])
        vH = int(vFields[5])
        aEView.drawTexts( vFields[6], vX-aContext._xorg, vY-aContext._yorg, vW, vH )
    
    else:
        vObjectId = int(vFields[1])
        vX = int(vFields[2])
        vY = int(vFields[3])
        vW = int(vFields[4])
        vH = int(vFields[5])
        aEView.drawObject( vObjectId, vX-aContext._xorg, vY-aContext._yorg, vW, vH )



def loadPosFileIntoEnlighten( aOptions, aPath, aObjproPath, aEFolderName, aEViewName ):
    # db stuff...
    vDbConfig = aOptions['db-config']
    vCnx = "host='{0}' port={1} dbname='{2}' user='{3}' password='{4}'".format(
        vDbConfig['db-server'], vDbConfig['db-port'], vDbConfig['db-base'], vDbConfig['db-login'], vDbConfig['db-password']
    )
    #with CCastEnlighten(vCnx,aOptions["db-schema-prefix"]+"_local") as vEnlighten:
    for iPosFile in ( aPath, ):
        logger.info( "-- Importing data from [{}] into Enligthen as [{}/{}]...".format(iPosFile,aEFolderName,aEViewName) )
        with psycopg2.connect(vCnx) as vConn:
            vEnlighten =  CCastEnlighten(vCnx,vDbConfig['db-local'])
            vEnlighten._conn = vConn
            vEnlighten._curs = vConn.cursor()
            vEFolderName = aEFolderName
            vEFolderId = vEnlighten.getEnlightenFolderId( vEFolderName )
            vEViewName = aEViewName
            vEView = vEnlighten.createValidView( vEFolderName, vEFolderId, vEViewName )

            vEView.doObjPro( getObjPro(aObjproPath) )

            #vEView.drawRectangle( 0,0, 100, 100 )
            #vEView.drawRectangle( 0,0, 200, 200 )

            vContext = Context()
            with open( aPath, "r" ) as vFPos:
                for iN, iLine in enumerate(vFPos):
                    vLine = iLine.strip()
                    if vLine and vLine[0]!="#":
                        parsePosLine( vContext, iN, vLine, vEView )
            vEnlighten._curs.close()

def renderTransactionAlgoInEnlighten2( aOptions, aObjProPath, aAlgo, aPosFilePath ):
    if os.path.isfile( aPosFilePath ):
        vEFolderName = "KyT_{}".format(aOptions["transaction-subfolder"])
        vEViewName = "algo_{}".format(aAlgo)
        loadPosFileIntoEnlighten( aOptions, aPosFilePath, aObjProPath, vEFolderName, vEViewName )

def renderTransactionInEnlighten2( aOptions ):
    if 'enable-enlighten' in aOptions["transaction-config"] and aOptions["transaction-config"]["enable-enlighten"]:
        vObjProPath = os.path.join( aOptions['tr-output-data-folder'], filenames.C_BASENAMES["objpro"] )
        for iA in ( '00', '01', '02', '03', '04', '05', '06', '07',
            'all-00', 'all-01', 'all-02', 'all-03', 'all-04', 'all-05',
            'all-06', 'all-07', 'all-08', 'all-09', 'all-10', 'all-11'):
            vPosFilePath = os.path.join( aOptions['tr-output-gviz-folder'], '99_objects-{}.txt.pos'.format(iA) )
            renderTransactionAlgoInEnlighten2( aOptions, vObjProPath, iA, vPosFilePath )
    else:
        logger.warning( "!Warning: skipping transaction [{}] cause enable-enlighten not set".format(aOptions['transaction']) )

def kytEnlighten2Main( aArgv ):
    if 0==len(aArgv) or not os.path.isfile( aArgv[0] ):
        logger.error( "***ERROR: No valid configuration file, exiting." )
        return
    vConfiguration = config.CConfig(aArgv[0])
    vConfiguration.processConfigurations(renderTransactionInEnlighten2,None,False)
