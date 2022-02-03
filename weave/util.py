# -*- coding: utf-8 -*-
"""
Created on Fri Jan 28 11:00:12 2022

@author: awatson
"""

import os

def pixToMB(pixNum,bit=16):
    return pixNum*bit/8/1024/1024

def MBToPix(MB,bit=16):
    return MB/bit*8*1024*1024

def getFileName(y,x):
    return '{}.{}.tif'.format(y,x)
    
def getDir(location,t,c,z):
    return os.path.join(
        location,
        str(t),
        str(c),
        str(z)
        )

def makeDir(location):
    os.makedirs(location, exist_ok=True)
    
def getFullFilePath(location,t,c,z,y,x):
    return os.path.join(
        getDir(location,t,c,z),
        getFileName(y,x)
                        )

def prepareAndGetFilePath(location,t,c,z,y,x):
    path = getFullFilePath(location,t,c,z,y,x)
    makeDir(os.path.split(path)[0])
    return path

def getMetaFile(location):
    return os.path.join(location,'weave.json')


def make5DimSlice(key,res,weaveNumber):
    '''
    Should take a slice key and return a tuple of slices of 5 dims (t,c,z,y,x)
    If resolution is specified in the input key by 6 dim (res,t,c,z,y,x) then 
    resolution will be extracted and returned.
    '''
    
    print('In key: {}'.format(key))
    # For a complete 6 dim slice, extract resolution level [0] and reform
    # the key as a 5 dim tuple        
    if not isinstance(key, (slice,int)) and len(key) == 6:
        res = key[0]
        if res >= weaveNumber:
            raise ValueError('Layer is larger than the number of ResolutionLevels')
        key = tuple((x for x in key[1::]))

    if isinstance(key, int):
        key = [slice(key)]
        
    # All slices must be converted to 5 dims and placed into a tuple
    if isinstance(key, slice):
        key = [key]

    # Convert int/slice mix to a tuple of slices
    elif isinstance(key, tuple):
        key = tuple((slice(x) if isinstance(x, (int,list)) else x for x in key))
    
    # Size tuple to len==5 by appending slice(None) to the end
    if len(key) < 5:
        key = tuple(
            (key[x] if len(key)>x else slice(None) for x in range(5))
            )
    else:
        tuple(key)
    
    print('Out key: {}'.format(key))
    ## NOTE: At this point, key should always be of len(key)==5 and each
    ## element should be a slice object.
    return key,res
    

    


