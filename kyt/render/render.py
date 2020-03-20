import os
import sys
import collections
import psycopg2
import json
import logging

from render import svg

logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.DEBUG
)

import subprocess

from common import timewatch
from common import config

TRenderFormats = collections.namedtuple( "TRenderFormats", [ "svg", "png", "dot", "xdot" ] )
RenderFormatNames = TRenderFormats(*TRenderFormats._fields)


class WinCmd:
    def __init__( self, aExePath, aArgs=None, aStdOut=None, aStdErr=None ):
        self._exePath = aExePath
        self._args = aArgs
        self._stdout = aStdOut
        self._sdterr = aStdErr

    def execute( self ):
        vCmd = self._formatCommand()
        logger.info( "       >>>>>> invoking command: {"+vCmd+"} ..." )
        return subprocess.call( vCmd )

    def setArguments( self, aArgs ):
        self._args = aArgs
        return self

    def _formatCommand( self ):
        # os.system() does not work when binary contains spaces and there
        # are options => use subprocess instead
        if self._stdout and self._stderr:
            retVal = '{} 2> "{}" > "{}"'.format( aCmd, aStdErr, aStdOut )
        else:
            retVal = self._exePath
        return retVal
        


def formatGVizCommand( aPathToDotExe, aFormat, aInFilepath, vOutFilepath=None ):
    retVal = '"{}" -T{} -O "{}"'.format(aPathToDotExe, aFormat, aInFilepath)
    return retVal

def renderGViz( aPathToDotExe, aInFilepath, aFormats ):
    if aFormats.png:
        WinCmd( formatGVizCommand( aPathToDotExe, RenderFormatNames.png, aInFilepath ) ).execute()

    if aFormats.svg:
        WinCmd( formatGVizCommand( aPathToDotExe, RenderFormatNames.svg, aInFilepath ) ).execute()
        
    if aFormats.dot:
        WinCmd( formatGVizCommand( aPathToDotExe, RenderFormatNames.dot, aInFilepath ) ).execute()
        
    if aFormats.xdot:
        WinCmd( formatGVizCommand( aPathToDotExe, RenderFormatNames.xdot, aInFilepath ) ).execute()

         

def renderTransaction( aOptions ):
    if "gviz-distrib-path" in aOptions:
        #vPathToDotExe = "{}\\bin\\dot.exe".format(aOptions["gviz-distrib-path"])
        vPathToDotExe = None
        for i in ( "dot.exe", os.path.join("bin","dot.exe"), os.path.join("bin","dot"), "dot" ):
            vPathToDotExe_ = os.path.join(aOptions["gviz-distrib-path"],i)
            if os.path.isfile(vPathToDotExe_):
                vPathToDotExe = vPathToDotExe_
                break
        if vPathToDotExe:
            vOutputFolderPath = aOptions['tr-output-gviz-folder']
            for iF in os.listdir(vOutputFolderPath):
                vGVizPath = os.path.join(vOutputFolderPath,iF)
                vBasename, vExtension =  os.path.splitext(iF)
                if os.path.isfile(vGVizPath) and vExtension.lower() in ( '.gviz', ):
                    logger.info( "    candidate found: [{}]".format(iF) )
                    renderGViz( vPathToDotExe, vGVizPath, TRenderFormats(True,True,False,False) )

                    vSvgPath = os.path.join(vOutputFolderPath,vBasename)+'.gviz.svg'
                    vSvgPos = os.path.join(vOutputFolderPath,vBasename)+'.pos'
                    vSvgJs = os.path.join(vOutputFolderPath,vBasename)+'.data.js'
                    logger.info( "      creating pos file [{}]".format(vSvgPos) )
                    logger.info( "      creating js file [{}]".format(vSvgJs) )
                    svg.svgToVis( vSvgPath, vSvgPos, vSvgJs )
        else:
            logg.error( "***ERROR: wrong 'gviz-distrib-path' in config files: {}".format(aOptions["gviz-distrib-path"]) )

    else:
        logg.error( "***ERROR: missing option 'gviz-distrib-path' in config file." )


def kytRenderMain( aArgv ):

    if 0==len(aArgv) or not os.path.isfile( aArgv[0] ):
        logger.error( "***ERROR: No valid configuration file, exiting." )
        return

    vConfiguration = config.CConfig(aArgv[0])

    vConfiguration.processConfigurations(renderTransaction,None,True)



if __name__ == "__main__":
    logger.info( "Starting..." )
    vWatch = timewatch.TimeWatch()
    vWatch.start()
    tryRenderMain( sys.argv[1:] )
    vWatch.stop()
    logger.info( "Finished: elapsed: {}, cpu: {}".format(vWatch.deltaElapsed(),vWatch.deltaCpu()) )