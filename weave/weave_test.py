# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from skimage import io
from itertools import product
from matplotlib import pyplot as plt
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

# file = r"C:\Users\alpha\OneDrive - University of Pittsburgh\Data Share\Alan\BrainA_test\composites\composite_z501_c561.tif"
# outLocation = r"C:\code\testOut"


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



subImages = {}
subImages['orig_shape'] = image.shape
subImages['orig_dtype'] = image.dtype
subImages['sub_sample'] = subSamp
for ii,oo in product(range(subSamp), range(subSamp)):
    # print('Reading {} {}'.format(ii,oo))
    subImages[(ii,oo)] = image[ii::subSamp, oo::subSamp]
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


# Determine the size of the image for the given resolution level
# Insert this info into the subImages dict as 'resolution{#}_shape'
y=0
x=0
for ii in range(subImages['sub_sample']):
    y+=subImages[(ii,0)].shape[0]
    x+=subImages[(0,ii)].shape[1]
    subImages['resolution{}_shape'.format(subImages['sub_sample']-ii-1)] = (y,x)

chunkSize = (512,512)

# Reassemble Full resolution (whole image)
canvas = np.zeros(subImages['orig_shape'], dtype=subImages['orig_dtype'])
for ii,oo in product(range(subImages['sub_sample']),range(subImages['sub_sample'])):
    canvas[ii::subImages['sub_sample'], oo::subImages['sub_sample']] = \
        subImages[(ii,oo)]




# Write full set
for ii in subImages:
    if isinstance(ii,tuple) == True:
        fileName = '{}.{}.tif'.format(ii[0],ii[1])
        fileName = os.path.join(outLocation,fileName)
        print(fileName)
        tifffile.imwrite(fileName, subImages[ii], tile=(chunkSize))
    else:
        pass
    
    
###############################################################################
## Reconstruct any resolution level of the whole image
# Resolution levels 0 (higest - fullres) - subImages['sub_sample']-1 (lowest)
# Higher resolution levels are lower resolution

resolutionLevel = 9
weaveNumber = subImages['sub_sample'] - resolutionLevel
 
# Form canvas and add woven data
z = np.zeros(subImages['resolution{}_shape'.format(resolutionLevel)],dtype=subImages['orig_dtype'])
for ii,oo in product(range(weaveNumber),range(weaveNumber)):
    z[ii::weaveNumber,oo::weaveNumber] = subImages[(ii,oo)]
    
###############################################################################

##  A Means to slice specific 'resolution levels' which will enable chunk
##  access to images on disk 
# Desired slice from a specific resolution level
ystart = 2000
ystop = 5032
ystep = 1
xstart = 2000
xstop = 3536
xstep = 1

resolutionLevel = 3


weaveNumber = subImages['sub_sample'] - resolutionLevel
currentShape = subImages['resolution{}_shape'.format(resolutionLevel)]

ratioToFullRes = []
for idx in range(2):
    ratioToFullRes.append(currentShape[idx]/subImages['orig_shape'][idx])

# Build canvas
currentShape = (
    len(range(currentShape[0])[ystart:ystop:ystep]),
    len(range(currentShape[0])[xstart:xstop:xstep]),
    )
x = np.zeros(currentShape,dtype=subImages['orig_dtype'])

## Transform to coordinates to small images for targeted reads
# Location in current Resolution / ratioToFullRes / subImages['sub_sample'] = location in small images
ystart = math.ceil(ystart/ratioToFullRes[0]/subImages['sub_sample'])
ystop = math.ceil(ystop/ratioToFullRes[0]/subImages['sub_sample'])

xstart = math.ceil(xstart/ratioToFullRes[1]/subImages['sub_sample'])
xstop = math.ceil(xstop/ratioToFullRes[1]/subImages['sub_sample'])



for ii,oo in product(range(weaveNumber),range(weaveNumber)):
    x[ii::weaveNumber,oo::weaveNumber] = subImages[(ii,oo)][ystart:ystop:ystep,xstart:xstop:xstep]


###############################################################################

# ystart = ystart // subImages['sub_sample']
# ystop = ystop // subImages['sub_sample']
# ystep = ystep
# xstart = xstart // subImages['sub_sample']
# xstop = xstop // subImages['sub_sample']
# xstep = xstep

# # Find lowres loctions
# ystart = ystart // subImages['sub_sample']
# ystop = ystop // subImages['sub_sample']
# ystep = ystep
# xstart = xstart // subImages['sub_sample']
# xstop = xstop // subImages['sub_sample']
# xstep = xstep




## Attempt at slicing to allow for targeted reading of files
# Slice inside fullres data, output = selected resolution level
# Resolution levels 0 (higest - fullres) - subImages['sub_sample']-1 (lowest)
# Higher resolution levels are lower resolution

for ii in range(11):
    resolutionLevel = ii
    weaveNumber = subImages['sub_sample'] - resolutionLevel
    
    # Slice inside of fullres data
    ystart = 5000
    ystop = 10000
    ystep = 1
    xstart = 5000
    xstop = 10000
    xstep = 1
    
    ystart = 5000//subImages['sub_sample']
    ystop = 10000//subImages['sub_sample']
    ystep = 1
    xstart = 5000//subImages['sub_sample']
    xstop = 10000//subImages['sub_sample']
    xstep = 1
    
    # Determine location in lowres images
    ySubset = range(subImages['orig_shape'][0])[ystart:ystop:ystep]
    xSubset = range(subImages['orig_shape'][1])[xstart:xstop:xstep]
    # Determine the size of the image for the given resolution level
    
    canvasShape = subImages['resolution{}_shape'.format(resolutionLevel)]
    
    # Form canvas and add woven data
    z = np.zeros(canvasShape,dtype=subImages['orig_dtype'])
    for ii,oo in product(range(weaveNumber),range(weaveNumber)):
        z[ii::weaveNumber,oo::weaveNumber] = subImages[(ii,oo)]
    
    io.imshow(z)
    plt.show()
    print(z.shape)

# # Read by tile
# with tifffile.imread(fileName, aszarr=True) as store:
#     za = zarr.open(store, mode='r')
#     tile = za[:128, :128]


    








