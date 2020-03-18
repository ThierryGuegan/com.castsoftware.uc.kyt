import sys
import os
import json
import time
import datetime
import collections
import traceback
import logging

logger = logging.getLogger(__name__) 
logging.basicConfig(
    format='[%(levelname)-8s][%(asctime)s][%(name)-12s] %(message)s',
    level=logging.INFO
)

from common import timewatch
from common import config 
from dal import n_dal
from common import n_graph
from model import n_data
from render import gviz
from process.dfsAlgo import algo_longest_path
from process.dfsAlgo import algo_longest_path2
from process.dfsAlgo import algo_paths_of_interest
from process.dfsAlgo import algo_paths_of_interest2

TAlgoDecl = collections.namedtuple( "TAlgoDecl", [ "name", "short_name", "module", "main"] )

C_PROCESS_CONFIG={
    "algorithms" : (
        # Longest riskiest path to tables only
        TAlgoDecl( "Longest riskiest path to table leaves, no cycle", "00", "process.dfsAlgo.algo_longest_path", "findLongestPathNoCycles" ),
        TAlgoDecl( "Longest riskiest path to table leaves, w/ cycle", "01", "process.dfsAlgo.algo_longest_path", "findLongestPathWithCycles" ),
        
        # Longest riskiest path to end points
        TAlgoDecl( "Longest riskiest path to end point leaves, no cycle", "02", "process.dfsAlgo.algo_longest_path2", "findLongestPathNoCycles2" ),
        TAlgoDecl( "Longest riskiest path to end point leaves, w/ cycle", "03", "process.dfsAlgo.algo_longest_path2", "findLongestPathWithCycles2" ),

        # Longest riskiest path to end points, another variant
        TAlgoDecl( "Longest riskiest path to to terminal node, no cycle", "04", "process.dfsAlgo.algo_paths_stop_at_terminal", "findPathesStopAtTerminal" ),

        # Longest riskiest path to end points, another variant
        #TAlgoDecl( "Longest riskiest path to to terminal node, no cycle", "05", "process.dfsAlgo.algo_paths_explore_paths", "explorePaths" ),
     )
}