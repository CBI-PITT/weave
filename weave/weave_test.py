# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from skimage import io
from itertools import product
import numpy as np
import os
import math
import tifffile
import zarr

def pixToMB(pixNum,bit=16):
    return pixNum*bit/8/1024/1024
def MBToPix(MB,bit=16):
    return MB/bit*8*1024*1024

file = r"Z:/testData/hooksBrain/composites_C1_Z0681.tif"
outLocation = r"Z:\testWeave"

file = r"C:\Users\alpha\OneDrive - University of Pittsburgh\Data Share\Alan\BrainA_test\composites\composite_z501_c561.tif"
outLocation = r"C:\code\testOut"


print('Reading {}'.format(file))
image = io.imread(file)


##  If I want chunks on disk to be no bigger than a certain size (0.5MB)
##  And I want low res images to be now larger than 10MB


##  Determine subsample rate to make each low res version <= maxLowRes
maxLowRes = 10
startSize = image.shape

for ii in range(1,max(image.shape)+1):
    # blockSize = ii*ii*16/8/1024/1024
    sizeSubSamp = math.ceil(image.shape[0] / ii) * math.ceil(image.shape[1] / ii)
    sizeSubSamp = pixToMB(sizeSubSamp)
    # print(sizeSubSamp)
    if sizeSubSamp <= maxLowRes:
        subSamp = ii
        print('Subsample rate = {}'.format(subSamp))
        break




ySub = subSamp
xSub = subSamp

subImages = {}
subImages['orig_shape'] = image.shape
subImages['orig_dtype'] = image.dtype
subImages['split'] = (ySub,xSub)
for ii,oo in product(range(ySub), range(xSub)):
    # print('Reading {} {}'.format(ii,oo))
    subImages[(ii,oo)] = image[ii::ySub, oo::xSub]
    # print(subImages[(ii,oo)].shape)
    

# Size of single low res
for idx in subImages:
    if isinstance(idx, tuple) != True:
        continue
    shapeSingleImg = subImages[idx].shape
    size = pixToMB(subImages[idx].shape[0] * subImages[idx].shape[1])
    
    break

print('Low-Res version = {} MB'.format(size))
print('Shape of single low res image = {}'.format(shapeSingleImg))





chunkSize = (512,512)

# Reassemble Full resolution (whole image)
canvas = np.zeros(subImages['orig_shape'], dtype=subImages['orig_dtype'])
for ii,oo in product(range(subImages['split'][0]),range(subImages['split'][1])):
    canvas[ii::subImages['split'][0], oo::subImages['split'][1]] = \
        subImages[(ii,oo)]




## Full Resolution
# Write full set
for ii in subImages:
    if isinstance(ii,tuple) == True:
        fileName = '{}.{}.tif'.format(ii[0],ii[1])
        fileName = os.path.join(outLocation,fileName)
        print(fileName)
        tifffile.imwrite(fileName, subImages[ii], tile=(chunkSize))
    else:
        pass
    

resolutionLevel = 5
outShape = (
    len(range(0,subImages['orig_shape'][0],resolutionLevel+1)),
    len(range(0,subImages['orig_shape'][1],resolutionLevel+1))
    )
z = np.zeros(outShape,dtype=subImages['orig_dtype'])

for ii,oo in product(range(resolutionLevel+1),range(resolutionLevel+1)):
    z[ii::outShape[0], oo::outShape[1]] = \
        subImages[(ii,oo)]
        
resolutionLevel = 5
outShape = (
    subImages[(0,0)].shape[0]*(resolutionLevel+1),
    subImages[(0,0)].shape[1]*(resolutionLevel+1)
    )
z = np.zeros(outShape,dtype=subImages['orig_dtype'])

for ii,oo in product(range(resolutionLevel+1),range(resolutionLevel+1)):
    z[ii::resolutionLevel+1, oo::resolutionLevel+1] = \
        subImages[(ii,oo)]


# resolutionLevel = 5
# y=0
# x=0
# for ii,oo in product(range(resolutionLevel),range(resolutionLevel)):
#     shape = subImages[(ii,oo)].shape
#     y+=shape[0]
#     x+=shape[1]
# outShape = (y,x)

# z = np.zeros(outShape,dtype=subImages['orig_dtype'])

# for ii,oo in product(range(resolutionLevel+1),range(resolutionLevel+1)):
#     z[ii::resolutionLevel+1, oo::resolutionLevel+1] = \
#         subImages[(ii,oo)]


# # Read by tile
# with tifffile.imread(fileName, aszarr=True) as store:
#     za = zarr.open(store, mode='r')
#     tile = za[:128, :128]


    








