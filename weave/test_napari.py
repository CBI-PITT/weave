# -*- coding: utf-8 -*-
"""
Created on Sun Feb  6 19:28:16 2022

@author: alpha
"""

import napari
import dask.array as da

location = r'c:\code\weave_out'
a = weave_read(location)

data = []
for ii in range(a.weaveNumber):
    data.append(weave_read(location, ResolutionLock=ii, delayed=True))
    
z = [da.from_array(x,chunks=x.chunks,fancy=False) for x in data]
# z = [x[0,0] for x in z]
# z = [da.from_array(x,chunks=(1,1,1,*x.shape[-2:]),fancy=False) for x in data]
viewer = napari.view_image(z,channel_axis=1)
