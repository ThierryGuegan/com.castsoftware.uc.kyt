# Copyright (C) 2020 You-Cast on Earth, Moon and Mars 2020
# This file is part of com.castsoftware.uc.kyt extension
# which is released under GNU GENERAL PUBLIC LICENS v3, Version 3, 29 June 2007.
# See file LICENCE or go to https://www.gnu.org/licenses/ for full license details.

import os
import pkg_resources



if "__main__" == __name__:
    # Test
    vRcName = "kyt/resources/_kyt.html"
    vRcName = "n_graph.py"
    print("Resource exists: ", pkg_resources.resource_exists(__name__,vRcName))
    vRcPath = pkg_resources.resource_filename( "kyt", vRcName )
    print( "Path to: ", vRcName, ":", vRcPath )