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
import math
import numpy as np
import tifffile
import imagecodecs
import zarr
import dask
from dask.delayed import delayed
import dask.array as da


from itertools import product
## weave specific imports
try:
    from .util import pixToMB, MBToPix, prepareAndGetFilePath, getFullFilePath, makeDir, getMetaFile
except Exception:
    try:
        from util import pixToMB, MBToPix, prepareAndGetFilePath, getFullFilePath, makeDir, getMetaFile
    except Exception:
        from weave.util import pixToMB, MBToPix, prepareAndGetFilePath, getFullFilePath, makeDir, getMetaFile

# location= r'/CBI_Hive/globus/pitt/bil/weave'
location = r'c:\code\weave_out'

'''
Currently this is working!

To use:
    -weave_read(location_of_dataset:str, ResolutionLock=some_int_up_to_weaveNumber)
    -All arrays are represented as 5-dim (t,c,z,y,x)
    -Slicing over 5D or fewer dimensions will extract data from the ResolutionLock (default 0)
    -Slicing over 6D will use dim 0 as the resolution  (res,t,c,z,y,x)
    -Fancy slicing is not yet implemented

To Do:
    -Resolution levels only downsample in 2D (y,x).  Need to change this to 3D.
        -Can implement by making ResolutionLock subsample z-layers
        -Could implement 3D weave
        -Could in principle implement n-dimensional weave
    -Parallelize reads
    -Is is possible to parallelize placing data onto canvas?
    
    -Implement caching of individual tiles so not to duplicate work for high-res reads
        -Implement RAM and persistant cache
    -Alternative storage formats?
    -Cusomizeable compression scheme?
    
'''

## A class to read weave
class weave_read:
    def __init__(self, location, ResolutionLock=0, delayed=False):
        '''
        Input array should be layed out as (t,c,z,y,x)
        '''
        
        self.location = location
        self.metaFile = os.path.join(location,'weave.json')
        
        with open(self.metaFile, 'r') as f:
            self.meta = json.load(f)
        
        # Convert lists to tuples
        for ii in self.meta:
            if isinstance(self.meta[ii],list):
                self.meta[ii] = tuple(self.meta[ii])
        
        self.dtype = np.dtype(self.meta['dtype'])
        self.compression = self.meta['compression']
        self.weaveNumber = self.meta['weaveNumber']
        self.setResolutionLock(ResolutionLock)
        
    def setResolutionLock(self,ResolutionLock):
        self.ResolutionLock = ResolutionLock
        self.shape = (
            *self.meta['shape'][:-2],
            *self.meta['resolution{}_shape'.format(self.ResolutionLock)]
            )
        self.size = math.prod(self.shape)
        self.ndim = 5
        
        '''
        Chunk size changes for each resolution level.  Although the chunks for each
        subresolution remains the same, for exsample (512,512), the effective chunk
        size increases in each dimension by the resolution level.  For example
        resolution_level4 must read 5 images for each dim.  Thus the subresolution 
        chunksize is multiplied by 5.  This makes reads more efficient.
        '''
        self.chunks = (
            1,1,1,
            self.meta['chunks'][0]*(self.weaveNumber-self.ResolutionLock),
            self.meta['chunks'][1]*(self.weaveNumber-self.ResolutionLock)
            )
        
    def __getitem__(self,key):
        
        print('In key: {}'.format(key))
        
        # Force a 5 dim slice representing (t,c,z,y,x)
        key,res = self.make5DimSlice(key)
        
        # Transform slices to have start,stop,step be completely filled.
        key = self.fillSlice(key)
        
        # Test whether slice is valid for the given resolution level based on
        # the size of resolution leve.  Error if not valid.
        self.isSliceValid(key,res)
        
        print('Get Slice: {}'.format(key))
        
        return self.getSlice(res,key)
                
    
    def make5DimSlice(self,key):
        '''
        Should take a slice key and return a tuple of slices of 5 dims (t,c,z,y,x)
        If resolution is specified in the input key by 6 dim (res,t,c,z,y,x) then 
        resolution will be extracted and returned.
        '''
        
        # print('In key: {}'.format(key))
        
        res = self.ResolutionLock
        # For a complete 6 dim slice, extract resolution level [0] and reform
        # the key as a 5 dim tuple        
        if not isinstance(key, (slice,int,np.integer)) and len(key) == 6:
            res = key[0]
            if res >= self.weaveNumber:
                raise ValueError('Layer is larger than the number of ResolutionLevels')
            key = tuple((x for x in key[1::]))

        if isinstance(key, (np.integer,int)):
            if isinstance(key,np.integer):
                key = key.item()
            key = [slice(key)]
            
        # All slices must be converted to 5 dims and placed into a tuple
        if isinstance(key, slice):
            key = [key]

        # Convert int/slice mix to a tuple of slices
        elif isinstance(key, tuple):
            key = tuple((slice(x) if isinstance(x, (int,np.integer,list)) else x for x in key))
        
        # Size tuple to len==5 by appending slice(None) to the end
        if len(key) < 5:
            key = tuple(
                (key[x] if len(key)>x else slice(None) for x in range(5))
                )
        else:
            tuple(key)
        
        # print('Out key: {}'.format(key))
        ## NOTE: At this point, key should always be of len(key)==5 and each
        ## element should be a slice object.
        return key,res
        
    
    def fillSlice(self,key):
        '''
        Takes a tuple of slices len==5 + resolutionLevel requested.  
        All None values are filled with relevant values
        '''
        
        newKey = []
        for key_idx,val in enumerate(key):
            start = val.start
            stop = val.stop
            step = val.step
            
            if step is None:
                step = 1
                
            if start is None and isinstance(stop, (np.integer,int)):
                start = stop
                stop += 1
            
            if start is None and stop is None:
                start = 0
                stop = self.shape[key_idx]
            
            newKey.append(slice(start,stop,step))
        
        return tuple(newKey)
            
    def isSliceValid(self,key,res):
        
        shape = self.meta['resolution{}_shape'.format(res)]
        
        shape = (*self.shape[:-2],*shape)
        # print(shape)
        # print(key)
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
    
    def readSubSampleSlice(self,t,c,z,y,x,ySlice,xSlice):
        # Read by slice 
        fileName = getFullFilePath(self.location,t,c,z,y,x)
        with tifffile.imread(fileName, aszarr=True) as store:
            za = zarr.open(store, mode='r')
            return za[ySlice, xSlice]
        
    ## Possibly depreciated below
    def pixelLocation(self,res,t,c,z,yCord,xCord):
        '''
        Take a location in the full resolution dataset and return a filename
        and coordinate for the pixel.
        
        Returns a tuple of (image_filename,y_cordinate,x_cordinate) 
        
        One use of this function is to find the boundries of a slice in low res
        versions.
        '''
        
        weaveNumber = self.weaveNumber - res
        
        imageY = yCord%weaveNumber
        imageYOffset = yCord//weaveNumber
        
        imageX = xCord%weaveNumber
        imageXOffset = xCord//weaveNumber
        
        return ( getFullFilePath(self.location,t,c,z,imageY,imageX), imageYOffset, imageXOffset )
        
    
    def getSlice(self,res,key):
        
        weaveNumber = self.weaveNumber - res
        # print(weaveNumber)
        
        outIter = tuple((range(x.start,x.stop,x.step) for x in key))
        # print(outIter)
        outShape = tuple((len(x) for x in outIter))
        
        
        
        # Determine the specific regions of low res images that must be read
        
        # Start values
        
        
        _,yStart,xStart = self.pixelLocation(res,0,0,0,outIter[-2][0],outIter[-1][0])
        _,yStop,xStop = self.pixelLocation(res,0,0,0,outIter[-2][-1],outIter[-1][-1])
        yStop += 1
        xStop += 1
        
        lowResYSlice = slice(yStart,yStop,outIter[-2].step)
        lowResXSlice = slice(xStart,xStop,outIter[-1].step)
        # print(lowResYSlice)
        # print(lowResXSlice)
        
        
        # Queue Read all images
        images = (
            delayed(self.readSubSampleSlice)
            (tt,cc,zz,ii,oo,lowResYSlice,lowResXSlice) 
            for tt,cc,zz,ii,oo in product(outIter[0],outIter[1],outIter[2],range(weaveNumber),range(weaveNumber))
         )
        
        # Read all images
        images = dask.compute(images)
        
        # Form canvas that weave data will be laid onto   
        canvas = np.zeros(outShape,dtype=self.dtype)
        # print(canvas.shape)
            
        idx = 0
        for idxt, __ in enumerate(outIter[0]):
            for idxc, __ in enumerate(outIter[1]):
                for idxz, __ in enumerate(outIter[2]):
                    for ii,oo in product(range(weaveNumber),range(weaveNumber)):
                        '''Read images off disk and build canvas'''
                        
                        ''' ***Need to parallelize the reads*** '''
                        
                        '''
                        The following code (yLen/xLen) trims images read off disk to fit the
                        canvas slice.  This is necessary because some images are 
                        smaller by 1 row/col.  Maybe future implementation should
                        save smaller images padded with a row/col with black to 
                        make this unnecessary
                        '''
                        
                        
                        # print('{}_{}_{}_{}_{}'.format(tt,cc,zz,ii,oo))
                        canvas[idxt,idxc,idxz,ii::weaveNumber,oo::weaveNumber] = images[idx][0][
                            0:len(range(outShape[-2])[ii::weaveNumber]),
                            0:len(range(outShape[-1])[oo::weaveNumber])
                            ]
        
        
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
        
        # ## PARALLEL READS, RAM INEFFICIENT
        # print('Making Canvas')
        # canvas = np.zeros(self.meta['resolution{}_shape'.format(resolutionLevel)],dtype=self.dtype)
        # print('Reading Images')
        # images = (
        #     delayed(self.readSubSample)(t,c,z,ii,oo) 
        #     for ii,oo in 
        #     product(range(weaveNumber),range(weaveNumber))
        #     )
        # images = dask.compute(images)[0]
        # # print(images)
        # # print(len(images))
        
        # # Can this be paralleled?
        # print('Laying Image on Canvas')
        # idx=0
        # for ii,oo in product(range(weaveNumber),range(weaveNumber)):
        #     canvas[ii::weaveNumber,oo::weaveNumber] = images[idx]
        #     idx+=1
        
        # return canvas
        return np.squeeze(canvas)
# a = weave_read(location)
# start = time.time();z=a[0];print(time.time()-start)

