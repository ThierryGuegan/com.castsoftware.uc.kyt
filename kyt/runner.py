# Copyright (C) 2020 You-Cast on Earth, Moon and Mars 2020
# This file is part of com.castsoftware.uc.kyt extension
# which is released under GNU GENERAL PUBLIC LICENS v3, Version 3, 29 June 2007.
# See file LICENCE or go to https://www.gnu.org/licenses/ for full license details.

import sys
import logging

logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.INFO
)

import extract.extract
import process.process
import render.render
import process.graph
import render.enlighten
import render.enlighten2


C_COMMANDS={
    "extract": extract.extract.kytExtractMain,
    "process": process.process.kytProcessMain,
    "graph": process.graph.kytGraphMain,
    "render": render.render.kytRenderMain,
    "enlighten": render.enlighten.kytEnlightenMain,
    "enlighten2": render.enlighten2.kytEnlighten2Main, 
}

def usage():
    logger.info( "Usage: runner.py {{command(s)}} {{config file path}} ")
    logger.info( "  command(s): {}".format(','.join(C_COMMANDS.keys())) )


def run( aArgs ):
    retVal = 0

    if aArgs:
        vConfigFilePath = aArgs[-1]
        vArgs = [ x.lower() for x in aArgs ]
        for i in C_COMMANDS.keys():
            if i in vArgs:
                C_COMMANDS[i]( [vConfigFilePath] )
    else:
        logger.error( "***ERROR: missing arguments" )
        usage()
        retVal = 1
    
    return retVal        



if __name__ == "__main__":
    run( sys.argv[1:] )