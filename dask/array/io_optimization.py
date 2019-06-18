from .. import config
import math
import numpy as np
import sys
sys.path.insert(0,'/home/user/Documents/workspace/projects/samActivities/tests/optimize_io')

import optimize_io
from optimize_io.main import *

def optimize_func(dsk, keys):
    dask_graph = dsk.dicts
    dask_graph = main(dask_graph)
    return dsk


