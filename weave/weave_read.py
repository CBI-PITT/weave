# -*- coding: utf-8 -*-
"""
Created on Mon Jan 31 20:34:30 2022

@author: alpha
"""

# from skimage import io
# from matplotlib import pyplot as plt
# import time
# import math
# import math
# import dask
# from dask.delayed import delayed
# from distributed import Client
# import gc
# # client = Client('c001.cbiserver:8786')

import os
import json
import numpy as np
import tifffile
import imagecodecs
import zarr
import dask
from dask.delayed import delayed


from itertools import product
## weave specific imports
try:
    from .util import pixToMB, MBToPix, prepareAndGetFilePath, getFullFilePath, makeDir, getMetaFile
except Exception:
    from weave.util import pixToMB, MBToPix, prepareAndGetFilePath, getFullFilePath, makeDir, getMetaFile

location= r'/CBI_Hive/globus/pitt/bil/weave'

## A class to read weave
class weave_read:
    def __init__(self, location, resolutionLock=0):
        '''
        Input array should be layed out as (t,c,z,y,x)
        '''
        
        self.location = location
        self.resolutionLock = resolutionLock
        self.metaFile = os.path.join(location,'weave.json')
        
        with open(self.metaFile, 'r') as f:
            self.meta = json.load(f)
        
        # Convert lists to tuples
        for ii in self.meta:
            if isinstance(self.meta[ii],list):
                self.meta[ii] = tuple(self.meta[ii])
        
        self.shape = self.meta['shape']
        self.size = self.meta['size']
        self.chunks = self.meta['chunks']
        self.dtype = np.dtype(self.meta['dtype'])
        self.compression = self.meta['compression']
        self.weaveNumber = self.meta['weaveNumber']
        
    def __getitem__(self,key):
        if isinstance(key,int):
            return self.getResolutionLevelZ(key,0,0,5000)
            
    def getResolutionLevelZ(self,resolutionLevel,t,c,z):
        
        
        ###############################################################################
        ## Reconstruct any resolution level of the whole image
        # Resolution levels 0 (higest - fullres) - subImages['sub_sample']-1 (lowest)
        # Higher resolution levels are lower resolution
        
        weaveNumber = self.weaveNumber - resolutionLevel
        print(weaveNumber)
        print(getFullFilePath(self.location,t,c,z,0,0))
         
        ## SINGLE THREADED
        # # Form canvas and add woven data
        # canvas = np.zeros(self.meta['resolution{}_shape'.format(resolutionLevel)],dtype=self.dtype)
        # for ii,oo in product(range(weaveNumber),range(weaveNumber)):
        #     canvas[ii::weaveNumber,oo::weaveNumber] = self.readSubSample(t,c,z,ii,oo)
        # return canvas
    
        ## VERY SLOW    
        # Form canvas and add woven data
        # canvas = da.zeros(self.meta['resolution{}_shape'.format(resolutionLevel)],dtype=self.dtype,chunks=(512,512))
        # for ii,oo in product(range(weaveNumber),range(weaveNumber)):
        #     y = len(range(self.shape[-2])[ii::self.weaveNumber])
        #     x = len(range(self.shape[-1])[oo::self.weaveNumber])
        #     canvas[ii::weaveNumber,oo::weaveNumber] = \
        #         da.from_delayed(delayed(self.readSubSample)(t,c,z,ii,oo),shape=(y,x),dtype=self.dtype)
        # return canvas.compute()
        
        ## PARALLEL READS, RAM INEFFICIENT
        print('Making Canvas')
        canvas = np.zeros(self.meta['resolution{}_shape'.format(resolutionLevel)],dtype=self.dtype)
        print('Reading Images')
        images = (
            delayed(self.readSubSample)(t,c,z,ii,oo) 
            for ii,oo in 
            product(range(weaveNumber),range(weaveNumber))
            )
        images = dask.compute(images)[0]
        # print(images)
        # print(len(images))
        
        print('Laying Image on Canvas')
        idx=0
        for ii,oo in product(range(weaveNumber),range(weaveNumber)):
            canvas[ii::weaveNumber,oo::weaveNumber] = images[idx]
            idx+=1
                
        return canvas
            
        
    def readSubSample(self,t,c,z,y,x):
        return tifffile.imread(getFullFilePath(self.location,t,c,z,y,x))
        

