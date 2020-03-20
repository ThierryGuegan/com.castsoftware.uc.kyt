import sys
import traceback
import json
import logging
import dal.css.tri
logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.INFO
)

import collections
import os
import pkg_resources
import shutil

import common.timewatch
TContext = collections.namedtuple( "TContext", [ "outputFolder" ] )

C_TRANSACTIONS_AUTOMATIC="automatic"
C_TRANSACTION_LIMITS={
    "limit":"ALL", "limit-rob":"ROB", "limit-eff":"EFF", "limit-sec":"SEC"
}
C_DEFAULT_LIMIT=30
C_KYT_RC_ROOT="../resources/"
C_KYT_VIEWER_RC_ROOT="kyt-html-viewer/"
C_KYT_VIEWER_RESOURCES=( "_kyt.html", "_kyt-png.html", "_kyt-svg.html", "_kyt-vis.html" , "transaction-data.js", "vis-network.min.css", "vis-network.min.js" )
C_KYT_VIEWER_FOLDER="[_kyt-viewer]"


class COptions:
    def __init__(self):
        self._options = {}

    def set( self, aKey, aValue ):
        self._options[aKey] = aValue

    def get( self, aKey, aDefault=None ):
        retVal = aDefault
        if aKey in self._options:
            retVal = self._options[aKey]
        return retVal


class CConfigUtil:
    def getOptionFromOPath( aOptions, aOPath, aDefault ):
        retVal = aDefault
        vDictOrVal = aOptions
        for i in aOPath.split('.'):
            if i in vDictOrVal:
                retVal = vDictOrVal[i]
                vDictOrVal = retVal
            else:
                retVal = aDefault
        return retVal
        
    def _createFolderIfNotExist( aFolderPath ):
        if not os.path.isdir( aFolderPath ):
            logger.info( "    creating folder [{0}]".format(aFolderPath) )
            os.mkdir( aFolderPath )



    def createConfigurationFrom( aConfig, aSchemaPrefix, aSchemaPrefixMeasure=None, aSchemaPrefixLocal=None, aSchemaPrefixMng=None, aSchemaPrefixCentral=None ):
        retVal = dict(aConfig)

        retVal['db-prefix'] = aSchemaPrefix
        retVal['db-measure'] = ( retVal['db-prefix'] + "_measure" ) if not aSchemaPrefixMeasure else aSchemaPrefixMeasure
        retVal['db-local'] = ( retVal['db-prefix'] + "_local" ) if not aSchemaPrefixLocal else aSchemaPrefixLocal
        retVal['db-central'] = ( retVal['db-prefix'] + "_central" ) if not aSchemaPrefixCentral else aSchemaPrefixCentral
        retVal['db-mngt'] = ( retVal['db-prefix'] + "_mngt" ) if not aSchemaPrefixMng else aSchemaPrefixMng

        #retVal['db-login'] = 'operator'
        #retVal['db-password'] = 'CastAIP'
        #retVal['db-base'] = 'postgres'

        return retVal


class CConfig:    
    def __init__( self, aPath ):
        self._configs = {}
        with open(aPath,"r") as vFD:
            self._configs = json.load( vFD )
            logger.info( "Found {} configurations".format(len(self._configs)))
            for i in self._configs:
                logger.debug( "  config: {}".format(i['config']))
            self._applyDefaultOptions()        

    def _applyDefaultOptions( self ):
        vDefaulftConfig = self._getDefaultConfig()
        if vDefaulftConfig:
            vDefaultOptions = vDefaulftConfig['options']
        for i in self._configs:
            if ":default:" != i['config']:
                for iK in vDefaultOptions.keys():
                    if iK not in i['options']:
                        i['options'][iK] = vDefaultOptions[iK]

    def _getDefaultConfig(self):
        retVal = None
        for i in self._configs:
            if ":default:" == i['config']:
                retVal = i
                break
        return retVal

    # skip disabled configurations
    def configurations( self, aSkipDisabled=True ):
        for i in self._configs:
            if ":default:" != i['config']:
                if not ( aSkipDisabled and 'enable' in i['options'] and True!=i['options']['enable'] ):
                    yield i
                else:
                    logger.warning( "!!!WARNING: skipping disabled config [{}]".format(i['config']) )

    def _prepareOutputDirectories( self, aConfigOptions ):
        CConfigUtil._createFolderIfNotExist( aConfigOptions['output-root-folder'] )

    def _prepareKytViewer( self, aConfigOptions ):
        vOutputRootFolder = aConfigOptions['output-root-folder']
        vKytViewerFolderPath = os.path.join( vOutputRootFolder, C_KYT_VIEWER_FOLDER )
        CConfigUtil._createFolderIfNotExist( vKytViewerFolderPath )
        for iRc in C_KYT_VIEWER_RESOURCES:
            vRcName = C_KYT_RC_ROOT + C_KYT_VIEWER_RC_ROOT + iRc
            if pkg_resources.resource_exists(__name__, vRcName):
                vDestPath = os.path.join( vKytViewerFolderPath, iRc )
                if not os.path.exists(vDestPath):
                    # Do not override by default
                    logger.info( "    copying kyt html viewer file [{}] to [{}]...".format(iRc,vKytViewerFolderPath) )
                    shutil.copy( pkg_resources.resource_filename(__name__,vRcName), vKytViewerFolderPath )

    def _prepareConfigurations( self, aConfigOptions ):
        self._prepareOutputDirectories( aConfigOptions )
        self._prepareKytViewer( aConfigOptions )

    def processConfigurations( self, aTrCallback, aConfigCallback, aUseCached ):
        for nC, iC in enumerate(self.configurations()):
            logger.info( "-- Processing config [{}]...".format(iC['config']) )
            vConfigOptions = iC['options']
            vWithViolations = True
            if "with-violations" not in vConfigOptions:
                vWithViolations = bool(vConfigOptions["with-violations"])
            vConfigOptions["with-violations"] = vWithViolations

            vDbConfig = { 'db-login':vConfigOptions['db-login'],
                'db-password':vConfigOptions['db-password'], 'db-server':vConfigOptions['db-server'],
                'db-port':vConfigOptions['db-port'], 'db-base':vConfigOptions['db-base']
            }
            vConfigOptions["db-config"] = vDbConfig
            vDbConfig = CConfigUtil.createConfigurationFrom( vDbConfig, vConfigOptions['db-schema-prefix'] )
            vConfigOptions["db-config"] = vDbConfig
            vErrors = []

            # Some preparation for present configuration
            self._prepareConfigurations( vConfigOptions )

            # Iterating trhtough transactions defined in config.json file
            vErrors.extend( self._processTransactions( iC, vConfigOptions, aTrCallback, aConfigCallback, aUseCached ) )

            if "disable-all-others" in vConfigOptions and vConfigOptions["disable-all-others"]:
                logger.warning( "!!!WARNING: skipping all other configurations: disable-all-others set in congif.json" )
                break

        if len(vErrors)>0:
            logger.error( "***ERROR: {} error(s):".format(len(vErrors)) )
            for iErr in vErrors:
                logger.error( "***ERROR  Config: {}, Transaction: {}, Error: {}: {}:\n{}".format(iErr[0], iErr[1], iErr[2][0], iErr[2][1], traceback.print_tb(iErr[2][2]) ) )
        else:
            logger.info( "no errors." )

    def _automaticLoadOfRiskiestTransactions( aConfigTransactions, aOptions ):
        retVal = []
        for nTr, iTrDef in enumerate(aConfigTransactions):

            # Check if automatic mode is on (handle only once)
            vAutomaticDone = False
            if C_TRANSACTIONS_AUTOMATIC in iTrDef and iTrDef[C_TRANSACTIONS_AUTOMATIC] and not vAutomaticDone:
                vLoadLimit = 0
                vLimits = { x[1] : C_DEFAULT_LIMIT for x in C_TRANSACTION_LIMITS.items() }
                for iL in C_TRANSACTION_LIMITS.items():
                    if iL[0] in iTrDef:
                        vLimits[iL[1]] = iTrDef[iL[0]]
                        if vLimits[iL[1]] > vLoadLimit:
                            vLoadLimit = vLimits[iL[1]]

                #logger.info( "DB-CONFIG: {}".format(aOptions["db-config"]) )
                vTTri = dal.css.tri.extractTransactionTri(aOptions["db-config"]["db-server"], aOptions["db-config"]["db-port"],
                    aOptions["db-config"]["db-login"], aOptions["db-config"]["db-password"],
                    aOptions["db-config"]["db-base"], aOptions["db-config"]["db-prefix"], vLoadLimit )
                for iHf in ( "ROB", "EFF", "SEC" ):
                    vLimit = vLimits[iHf]
                    vTransactionAlreadyDeclared = set()
                    logger.info( "###### {}: top {} riskiest transactions:".format(iHf,vLimit) )
                    iN = 0
                    for iTTri in vTTri[iHf]:
                        if iN < vLimit:
                            logger.info( "  {:<2}: TRI: {}, transaction fullname: {}".format(iN+1,iTTri.tri,iTTri.fullname) )
                            if iTTri.fullname not in vTransactionAlreadyDeclared:
                                vAutoTr = {
                                    "#":"{}, {}, TRI={}".format(iHf,iN+1,iTTri.tri),
                                    "subfolder":"[{}]_{:0>2}".format(iHf,iN+1),
                                    "fullname":"{}".format(iTTri.fullname),
                                    "enable":True,
                                    "enable-enlighten":True if iN<6 else False,
                                    "root-object-id":None
                                }
                                #logger.info( "    -> {}".format(vAutoTr) )
                                vTransactionAlreadyDeclared.add( iTTri.fullname )
                                retVal.append( vAutoTr )
                                iN += 1
                            else:
                                logger.warning( "!!!WARNING: transaction already declared, skippint it: {}".format(iTTri.fullname) )
                vAutomaticDone = True
        return retVal

    def _generateListOfRiskiestTransactions( aConfigTransactions, aOptions, aUseCached ):
        vAutoTransactions = []
        vAutoTransactionsFilePath = os.path.join(aOptions['output-root-folder'],"transaction-decl.json")
        if aUseCached:
            logger.info( "  reading cached auto transactions from file [{}]".format(vAutoTransactionsFilePath) )
            with open(vAutoTransactionsFilePath,"r") as vInJson:
                for iLine in vInJson:
                    vLine = iLine.strip()
                    if vLine[-1]==',': vLine = vLine[:-1]
                    vTrDecl = json.loads(vLine)
                    vAutoTransactions.append( vTrDecl )
        else:
            vAutoTransactions = CConfig._automaticLoadOfRiskiestTransactions(aConfigTransactions,aOptions)
            logger.info( "  saving auto transactions into file [{}]".format(vAutoTransactionsFilePath) )
            with open(vAutoTransactionsFilePath,"w") as vOutJson:
                for iN, iTrDecl in enumerate(vAutoTransactions):
                    print( "  {}{}".format(json.dumps(iTrDecl),',' if iN<len(vAutoTransactions)-1 else ''), file=vOutJson )
        retVal = vAutoTransactions.copy()
        retVal.extend( [ x for x in aConfigTransactions if C_TRANSACTIONS_AUTOMATIC not in x ] )
        return retVal

    def _processTransactions( self, aConfig, aOptions, aTrCallback, aConfigCallback, aUseCached ):
        vConfigTransactions = CConfig._generateListOfRiskiestTransactions(aConfig['transactions'],aOptions,aUseCached)

        vKytViewerFolderPath = os.path.join( aOptions['output-root-folder'], C_KYT_VIEWER_FOLDER )
        with open( os.path.join(vKytViewerFolderPath,"transaction-data.js"), "w" ) as vJsTrData:
            # Output algorithms
            print( "var G_Algorithms=[\n];", file=vJsTrData )
            
            # Output transactions information for kyt-viewer
            print( "var G_Transactions=[", file=vJsTrData )

            for nTr, iTrDef in enumerate(vConfigTransactions):
                # Copy transaction configuration to be accessible by callbak
                aOptions['transaction-config'] = iTrDef.copy()
            
                vCriteria = None
                vTransaction = None

                for i in [ ('id',lambda x:int(x)), ('name',lambda x:x), ('fullname',lambda x:x)]:
                    if i[0] in iTrDef:
                        aOptions['transaction-criteria'] = i[0]
                        aOptions['transaction'] = i[1](iTrDef[i[0]].strip())
                        aOptions['transaction-criteria-value'] = aOptions['transaction']
                        aOptions['transaction-criteria-op'] = "="
                        break

                if 'transaction' not in aOptions:
                    logger.error( "***ERROR: missing transaction criteria for transaction {}: should be 'id', 'name' or 'fullname'".format(nTr) )
                    continue

                if 'subfolder' in iTrDef:
                    vSubfolder = iTrDef['subfolder'].strip()
                else:
                    vSubfolder = vTransaction

                aOptions['transaction-subfolder'] = vSubfolder
                if 'enable' in iTrDef and "false"==str(iTrDef['enable']).lower():
                    logger.warning( "!!!WARNING: skipping disabled transaction [{}]".format(vTransaction) )
                    continue

                aOptions['tr-output-folder'] = os.path.join(aOptions['output-root-folder'],vSubfolder)
                aOptions['tr-output-data-folder'] = os.path.join(aOptions['tr-output-folder'],"_data")
                aOptions['tr-output-gviz-folder'] = os.path.join(aOptions['tr-output-folder'],"_gviz")
                aOptions['tr-output-paths-folder'] = os.path.join(aOptions['tr-output-folder'],"_paths")

                logger.info( "  -- Processing transaction [{0}] into [{1}]...".format(aOptions['transaction'],aOptions['tr-output-folder']) )
                for iO in ('transaction-criteria','transaction','output-root-folder','tr-output-folder','tr-output-data-folder',
                    'tr-output-gviz-folder', 'tr-output-paths-folder' ):
                    logger.info( "    ###### {} : {}".format(iO,aOptions[iO]) )
                
                for iF in ( 'output-root-folder', 'tr-output-folder', 'tr-output-data-folder', 'tr-output-gviz-folder', 'tr-output-paths-folder' ):
                    CConfigUtil._createFolderIfNotExist( aOptions[iF] )

                # Call processor
                vWatch = common.timewatch.TimeWatch()
                vErrors = []
                try:
                    aTrCallback( aOptions )
                except ( AssertionError, TypeError, NameError, AttributeError ):
                    raise
                except:
                    logger.error( "***ERROR: {}: {}:\n{}".format(sys.exc_info()[0],sys.exc_info()[1],traceback.print_tb(sys.exc_info()[2])) )
                    vErrors.append( ( aConfig['config'], vSubfolder, sys.exc_info() ) )
                print( '  {{ "cast-hf":"*unknown*", "cast-tri":-1, "subfolder":"{}", "cast-fullname":"*unknown*" }},'.format(vSubfolder), file=vJsTrData )
                vWatch.stop()

                logger.info( "    transaction-dump: elapsed: {}, cpu: {}".format(vWatch.deltaElapsed(),vWatch.deltaCpu()) )            
                if "transaction-limit" in aOptions and aOptions["transaction-limit"]!=None and nTr>=int(aOptions["transaction-limit"]-1):
                    logger.warning( "!!!WARNING: skipping all other transactions: transaction-limit set to {} in congif.json".format(aOptions["transaction-limit"]) )
                    break
            
            print( "];", file=vJsTrData )
        return vErrors
