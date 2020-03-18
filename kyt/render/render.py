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

from common import timewatch
from common import config

TRenderFormats = collections.namedtuple( "TRenderFormats", [ "svg", "png", "dot", "xdot" ] )
RenderFormatNames = TRenderFormats(*TRenderFormats._fields)

class SvgToJvis:
    def __init__( self, aSvgFilepath ):
        self._path = aSvgFilepath
    

def execCmd( aCmd, aStdOut="", aStdErr="" ):
    if aStdOut and aStdErr:
        vCmd = aCmd + ' 2> "{0}" > "{1}"'.format( aStdErr, aStdOut )
    else:
        vCmd = aCmd
    logger.info( "      >>>>>> invoking command: {"+vCmd+"} ..." )
    os.system( vCmd )


def formatGVizCommand( aPathToDotExe, aFormat, aInFilepath, vOutFilepath=None ):
    retVal = '{} -T{} -O "{}"'.format(aPathToDotExe, aFormat, aInFilepath)
    return retVal

def renderGViz( aPathToDotExe, aInFilepath, aFormats ):
    if aFormats.png:
        vCmd = formatGVizCommand( aPathToDotExe, RenderFormatNames.png, aInFilepath )
        execCmd( vCmd )

    if aFormats.svg:
        vCmd = formatGVizCommand( aPathToDotExe, RenderFormatNames.svg, aInFilepath )
        execCmd( vCmd )
        
    if aFormats.dot:
        vCmd = formatGVizCommand( aPathToDotExe, RenderFormatNames.dot, aInFilepath )
        execCmd( vCmd )
        
    if aFormats.xdot:
        vCmd = formatGVizCommand( aPathToDotExe, RenderFormatNames.xdot, aInFilepath )
        execCmd( vCmd )
        

def renderTransaction( aOptions ):
    if "gviz-distrib-path" in aOptions:
        vPathToDotExe = "{}\\bin\\dot.exe".format(aOptions["gviz-distrib-path"])
        
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
        logg.error( "***ERROR: could not find option 'gviz-distrib-path' in config file." )


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