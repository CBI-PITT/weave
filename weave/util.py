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




    


