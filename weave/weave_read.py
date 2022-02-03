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
    from .util import pixToMB, MBToPix, prepareAndGetFilePath, getFullFilePath, makeDir, getMetaFile,make5DimSlice
except Exception:
    try:
        from util import pixToMB, MBToPix, prepareAndGetFilePath, getFullFilePath, makeDir, getMetaFile,make5DimSlice
    except Exception:
        from weave.util import pixToMB, MBToPix, prepareAndGetFilePath, getFullFilePath, makeDir, getMetaFile,make5DimSlice

location= r'/CBI_Hive/globus/pitt/bil/weave'
# location = r'c:\code\weave_out'

## A class to read weave
class weave_read:
    def __init__(self, location, ResolutionLock=0):
        '''
        Input array should be layed out as (t,c,z,y,x)
        '''
        
        self.location = location
        self.ResolutionLock = ResolutionLock
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
        
        # Force a 5 dim slice representing (t,c,z,y,x)
        key,res = make5DimSlice(key,self.ResolutionLock,self.weaveNumber)
        
        # Transform slices to have start,stop,step be completely filled.
        key = self.fillSlice(key)
        
        # Test whether slice is valid for the given resolution level based on
        # the size of resolution leve.  Error if not valid.
        self.isSliceValid(key,res)
        
        
        '''
        NOTE: At this point, key should always be of len(key)==5 and each
        element should be a slice object.
        
        res will indicate the resolution level from which to extract data.
        '''
        
        ##  Need to work on a way to extract 3D accross Z
        if all(x==slice(None) for x in key[-2::]):
            pass
            # return self.getResolutionLevelZ(res,0,0,key) #<-- not correct
                
    
    def fillSlice(self,key):
        '''
        Takes a tuple of slices len==5 + resolutionLevel requested.  
        All None values are filled with relevant values
        '''
        
        newKey = []
        for val in key:
            start = val.start
            stop = val.stop
            step = val.step
            
            if step is None:
                step = 1
                
            if start is None and isinstance(stop, int):
                start = stop
                stop += 1
            
            newKey.append(slice(start,stop,step))
        
        return tuple(newKey)
            
    def isSliceValid(self,key,res):
        
        shape = self.meta['resolution{}_shape'.format(res)]
        
        for dim,_ in enumerate(key):
            if key[dim].stop > shape[dim]+1:
                raise ValueError('Stop value of {} for dimension {} is bigger than the size of the array: {}'.format(key[dim].stop,dim,shape[dim]))
            if key[dim].start > shape[dim]+1:
                raise ValueError('Start value of {} for dimension {} is bigger than the size of the array: {}'.format(key[dim].start,dim,shape[dim]))
            
        
        
        
        
    def getResolutionLevelZ(self,resolutionLevel,t,c,z):
        
        
        ###############################################################################
        ## Reconstruct any resolution level of a the whole 'z' image
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
        
        # Can this be paralleled?
        print('Laying Image on Canvas')
        idx=0
        for ii,oo in product(range(weaveNumber),range(weaveNumber)):
            canvas[ii::weaveNumber,oo::weaveNumber] = images[idx]
            idx+=1
                
        return canvas
            
        
    def readSubSample(self,t,c,z,y,x):
        return tifffile.imread(getFullFilePath(self.location,t,c,z,y,x))
        
    ## Possibly depreciated below
    def pixelLocation(self,t,c,z,yCord,xCord):
        '''
        Take a location in the full resolution dataset and return a filename
        and coordinate for the pixel.
        
        Returns a tuple of (image_filename,y_cordinate,x_cordinate) 
        '''
        
        imageY = yCord%self.weaveNumber
        imageYOffset = yCord//self.weaveNumber
        
        imageX = xCord%self.weaveNumber
        imageXOffset = xCord//self.weaveNumber
        
        return ( getFullFilePath(self.location,t,c,z,imageY,imageX), imageYOffset, imageXOffset )
        
    
    def getSlice(self,res,key):
        
        resolutionLevelShape = self.meta['resolution{}_shape'.format(res)]
        weaveNumber = self.weaveNumber - res
        
        outIter = tuple((range(x.start,x.stop,x.step) for x in key))
        outShape = tuple((len(x) for x in outIter))
        
        canvas = np.zeros(outShape,dtype=self.dtype)
        
        for tt,cc,zz,yy,xx in product(*outIter):
            '''
            Read images off disk and build canvas
            '''
        
        
        
        
        ###############################################################################
        ## Reconstruct any resolution level of a the whole 'z' image
        # Resolution levels 0 (highest - fullres) - subImages['sub_sample']-1 (lowest)
        # Higher resolution levels are lower resolution
        
        
         
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
        
        # Can this be paralleled?
        print('Laying Image on Canvas')
        idx=0
        for ii,oo in product(range(weaveNumber),range(weaveNumber)):
            canvas[ii::weaveNumber,oo::weaveNumber] = images[idx]
            idx+=1
                
        return canvas
# a = weave_read(location)
# start = time.time();z=a[0];print(time.time()-start)

