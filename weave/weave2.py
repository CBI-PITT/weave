# -*- coding: utf-8 -*-
"""
Created on Fri Jan 28 10:01:15 2022

@author: awatson
"""

from skimage import io
from itertools import product
from matplotlib import pyplot as plt
import numpy as np
import os
import math
import json
import math
import tifffile
import imagecodecs
import zarr
import dask
from dask.delayed import delayed
from distributed import Client
# client = Client('c001.cbiserver:8786')

## weave specific imports
from .util import pixToMB, MBToPix, prepareAndGetFilePath, getFullFilePath, makeDir, getMetaFile

testInput = np.zeros((2,10,30000,30000), dtype=np.uint16)
location= r'z:\testWeave'


## A class to create and read weave
class weave_make:
    def __init__(self, inputArray, saveLocation, maxLowResMB=10, chunks=(512,512), compression='zlib', client=None):
        '''
        Input array should be layed out as (t,c,z,y,x)
        '''
        
        while len(inputArray.shape) < 5:
            inputArray = inputArray[None,...]
            
        self.inputArray = inputArray
        self.shape = inputArray.shape
        try:
            self.size = inputArray.size
        except Exception:
            self.size = math.prod(self.shape)
        
        self.location = saveLocation
        self.maxLowResMB = maxLowResMB
        self.dtype = str(inputArray.dtype) # str allows us to serialize to json
        
        self.chunks = chunks
        self.compression = compression
        
        self.client = client
        
        ## Create meta dict which will be saved to disk to descrive weave array
        self.meta = {}
        self.meta['shape'] = self.shape
        self.meta['size'] = self.size
        self.meta['location'] = self.location
        self.meta['maxLowResMB'] = self.maxLowResMB
        self.meta['dtype'] = str(self.dtype)
        self.meta['chunks'] = self.chunks
        self.meta['compression'] = self.compression
        
        
        
        ## Determine the proper weave number (ie subsample number)
        for ii in range(1,max(self.shape[-2::])+1):
            # blockSize = ii*ii*16/8/1024/1024
            sizeSubSamp = math.ceil(self.shape[-2] / ii) * math.ceil(self.shape[-1] / ii)
            sizeSubSamp = pixToMB(sizeSubSamp)
            # print(sizeSubSamp)
            if sizeSubSamp <= self.maxLowResMB:
                subSamp = ii
                print('Subsample rate = {}'.format(subSamp))
                break
        self.meta['weaveNumber'] = subSamp
        self.meta['lowResSizeMB'] = sizeSubSamp
        self.meta['total_file_count'] = math.prod(self.meta['shape'][:-2]) * self.meta['weaveNumber']**2
        self.meta['size_uncompressedTB'] = pixToMB(self.size) / 1024 / 1024
        
        # Determine the size of the image for the given resolution level
        # Insert this info into the meta dict as 'resolution{#}_shape'
        y,x = 0,0
        yRange = tuple(range(self.shape[-2]))
        xRange = tuple(range(self.shape[-1]))
        for ii in range(self.meta['weaveNumber']):
            y += len(yRange[ii::self.meta['weaveNumber']])
            x += len(xRange[ii::self.meta['weaveNumber']])
            self.meta['resolution{}_shape'.format(self.meta['weaveNumber']-ii-1)] = (y,x)
        
        
        
        # Run Save
        makeDir(os.path.split(getMetaFile(self.meta['location']))[0])
        with open(getMetaFile(self.meta['location']), 'w') as fp:
            json.dump(self.meta, fp, indent=2)
        self.makeWeave()


    
    def makeWeave(self):
        toWrite = []
        for t,c,z in product(range(self.meta['shape'][0]),
                                   range(self.meta['shape'][1]),
                                   range(self.meta['shape'][2])
                                   ):
            
            print('Queueing z-layer {}'.format(z))
            
            # toWrite.append(
            #     delayed(self.saveTiff)
            #     (fileName,
            #       self.inputArray[t,c,z,ii::self.meta['weaveNumber'], oo::self.meta['weaveNumber']],
            #       tile=self.meta['chunks'],
            #       compression=self.meta['compression']
            #       )
            #     )
            
            toWrite.append(
                delayed(self.writeZ)
                (
                self.inputArray[t,c,z],
                 t,c,z
                  )
                )
            
        print('Computing Saves')
        print(toWrite)
        if self.client is None:
            dask.compute(toWrite)
        elif self.client == 'local':
            client = Client()
            making = client.compute(toWrite)
            making = client.gather(making)
        else:
            client = Client(self.client)
            making = client.compute(toWrite)
            making = client.gather(making)
    
    
    def writeZ(self,array,t,c,z):
        fileName = prepareAndGetFilePath(self.location,t,c,z,0,0)
        for ii,oo in product(range(self.meta['weaveNumber']),range(self.meta['weaveNumber'])):
            fileName = getFullFilePath(self.location,t,c,z,ii,oo)
            print('Writing {}'.format(fileName))
            tifffile.imwrite(
                fileName,
                array[ii::self.meta['weaveNumber'], oo::self.meta['weaveNumber']],
                tile=self.meta['chunks'],
                compression=self.meta['compression']
                )
        return None



# # Size of single low res
# for idx in subImages:
#     if isinstance(idx, tuple) != True:
#         continue
#     shapeSingleImg = subImages[idx].shape
#     size = pixToMB(subImages[idx].shape[0] * subImages[idx].shape[1])
    
#     break

# print('Low-Res version = {} MB'.format(size))
# print('Shape of single low res image = {}'.format(shapeSingleImg))




# chunkSize = (512,512)

# # Reassemble Full resolution (whole image)
# canvas = np.zeros(self.meta['shape'], dtype=self.meta['dtype'])
# for ii,oo in product(range(self.meta['weaveNumber']),range(self.meta['weaveNumber'])):
#     canvas[ii::self.meta['weaveNumber'], oo::self.meta['weaveNumber']] = \
#         subImages[(ii,oo)]




