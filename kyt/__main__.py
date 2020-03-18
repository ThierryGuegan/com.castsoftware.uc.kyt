# __main__.py Copyright (C) 2020 You-Cast on Earth, Moon and Mars 2020
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


import kyt.runner



if __name__ == "__main__":
    logger.info( "Starting..."+str(sys.argv[1:]) )
    kyt.runner.run( sys.argv[1:] )
    logger.info( "Finished." )