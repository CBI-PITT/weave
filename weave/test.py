# -*- coding: utf-8 -*-
"""
Created on Fri Jan 28 15:19:18 2022

@author: awatson
"""
from weave import weave5 as wve
from imaris_ims_file_reader import ims
from skimage import io
from dask.delayed import delayed
import dask.array as da
import glob
import os
import time
import math

# inputArray = r"Z:\testData\bitplaneConverter.ims"
outputLocation = r"z:\testWeave"

outputLocation = '/CBI_Hive/globus/pitt/bil/weave'

ch1 = '/CBI_Hive/globus/pitt/bil/CH1'
ch2 = '/CBI_Hive/globus/pitt/bil/CH2'

ch1 = '/CBI_Hive/globus/pitt/bil/TEST/CH1'
ch2 = '/CBI_Hive/globus/pitt/bil/TEST/CH2'
outputLocation = '/CBI_Hive/globus/pitt/bil/TEST/weave'

ch1 = '/CBI_Hive/globus/pitt/bil/CH1'
ch2 = '/CBI_Hive/globus/pitt/bil/CH2'
outputLocation = '/CBI_Hive/globus/pitt/bil/weave'

#ch1 = '/CBI_FastStore/testWeave/TEST/CH1'
#ch2 = '/CBI_FastStore/testWeave/TEST/CH2'
#outputLocation = '/CBI_FastStore/testWeave'


ch1 = sorted(glob.glob(os.path.join(ch1,'*.tif')))
ch2 = sorted(glob.glob(os.path.join(ch2,'*.tif')))

print('Reading Sample Image')
testImage = io.imread(ch1[0])



ch1 = [delayed(io.imread)(x) for x in ch1]
ch2 = [delayed(io.imread)(x) for x in ch2]

ch1 = [da.from_delayed(x,testImage.shape, dtype=testImage.dtype) for x in ch1]
ch2 = [da.from_delayed(x,testImage.shape, dtype=testImage.dtype) for x in ch2]

ch1 = da.stack(ch1)
ch2 = da.stack(ch2)

array = da.stack((ch1,ch2))

startTime = time.time()
# z = wve.weave_make(array,outputLocation, client='c001.cbiserver:8786')
z = wve.weave_make(array,outputLocation, compression = 'zlib', client='local',batchSize=24)

stopTime = time.time()


# startTime = time.time()
# z = weave2.weave_make(array,outputLocation)
# stopTime = time.time()

print('Time to convert {}TB was {} hours'.format(z.meta['size_uncompressedTB'], (stopTime-startTime)/60/60))

## HiveComp2 only - 'zlib'
#Time to convert 0.4689457703389053TB was 1.0057191585832173 hours  ## Hive to Hive  ## Variable CPU
#Time to convert 0.4689457703389053TB was 1.0133128878143098 hours  ## Hive to FastStore ## Variable CPU
#Time to convert 0.4689457703389053TB was 1.0063590422603819 hours  ## FastStore to FastStore ## Variable CPU

## HiveComp2 only - local distrubuited client - 'zlib'
#Time to convert 0.4689457703389053TB was 0.34651788519488436 hours  ## FastStore to FastStore  ##  100% CPU for entire run
#Time to convert 0.4689457703389053TB was 0.3433335017495685 hours  ## Hive to Hive  ##  100% CPU for entire run

## HiveComp2 only - local distribuited client NO compression
#Time to convert 0.4689457703389053TB was 0.2358036106162601 hours  ## Hive to Hive  ## 75% CPU
