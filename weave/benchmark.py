# -*- coding: utf-8 -*-
"""
Created on Sun Feb  6 13:40:46 2022

@author: alpha
"""

import time
import dask.array as da
## Benchmark

times = 10
timeNorm = []
timeDask = []

a = weave_read(location)
for ii in range(a.weaveNumber-1,0,-1):
    a.setResolutionLock(ii)
    z = da.from_array(a)
    
    start = time.time()
    for _ in range(times):
        tmp = a[0,0,5]
    end = time.time()
    timeNorm.append(round((end-start)/times,2))
    
    start = time.time()
    for _ in range(times):
        tmp = z[0,0,5].compute()
    end = time.time()
    timeDask.append(round((end-start)/times,2))
        
        
    
    

    