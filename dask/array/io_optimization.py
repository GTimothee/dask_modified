from .. import config
import math
import numpy as np
import sys
sys.path.insert(0,'/home/user/Documents/workspace/projects/samActivities/tests/optimize_io')
import time 
import optimize_io
from optimize_io.main import *

def optimize_func(dsk, keys):
    t = time.time()
    dask_graph = dsk.dicts
    dask_graph = clustered_optimization(dask_graph)
    t = time.time() - t
    print("time to create graph:", t)
    return dsk


