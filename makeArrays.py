# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 12:15:28 2021

@author: jbarclay
"""

import os
import numpy as np
import pandas as pd
import xarray as xr


def tardir(path, tar_name):
    import tarfile
    with tarfile.open(tar_name, "w") as tar_handle:
        for root, dirs, files in os.walk(path):
            for file in files:
                tar_handle.add(os.path.join(root, file))

def makeArrays(arrayName, fileName=[], subSetList=[""],subsetDict = {}, tarFiles = False, outPath="",segsToExclude=[], suffix = "", qaDict={}):    
    #read in the data
    tempDF = pd.read_csv(fileName[0].replace(".zip",".csv"))
    colsToDrop = ['subseg_id','site_id','in_time_holdout','in_space_holdout','test','min_temp_c',
       'max_temp_c']
    if any([x in tempDF.columns for x in colsToDrop]):
        tempDF.drop(columns=colsToDrop, errors="ignore", inplace=True)
        
    #change the date formate
    if "date" in tempDF.columns:
        tempDF.date=tempDF.date.astype('datetime64[ns]')
#        tempDF['date'] = pd.to_datetime(tempDF.date, utc=True)
        
    if len(fileName)>1:
        for thisFile in fileName[1:]:
            tempDF2 = pd.read_csv(thisFile.replace(".zip",".csv"))
            #change the date formate
            if "date" in tempDF2.columns:
                tempDF2.date=tempDF2.date.astype('datetime64[ns]')
#                tempDF2['date'] = pd.to_datetime(tempDF2.date, utc=True)
            tempDF = tempDF.merge(tempDF2,how="outer", on=["seg_id_nat","date"])
            #this is repeated b/c they may have been introduced by the merge
            if any([x in tempDF.columns for x in colsToDrop]):
                tempDF.drop(columns=colsToDrop, errors="ignore", inplace=True)

    #remove excluded segments
    tempDF = tempDF[~tempDF.seg_id_nat.isin(segsToExclude)]
    
    #change the column name of water
    if "mean_temp_c" in tempDF.columns:
        tempDF.rename(columns={'mean_temp_c':'temp_c'},inplace=True)
    
    outTxt = arrayName
    outTxt = outTxt + "\n\n"+"Data Summary"
    outTxt = outTxt + "\n\n"+"Number of rows: " + str(tempDF.shape[0])

    #ensure the variables are reasonable
    for thisVar in qaDict.keys():
        if thisVar in tempDF.columns:
            tempDF.loc[(tempDF[thisVar]<(qaDict[thisVar]["min"])),thisVar]=np.nan
            tempDF.loc[(tempDF[thisVar]>(qaDict[thisVar]["max"])),thisVar]=np.nan
        
#    if "seg_tave_air" in tempDF.columns:
#        tempDF.loc[(tempDF.seg_tave_air<(-50)),"seg_tave_air"]=np.nan
#        tempDF.loc[(tempDF.seg_tave_air>(50)),"seg_tave_air"]=np.nan
        
    for thisCol in tempDF.columns:
        if thisCol!="seg_id_nat":
            outTxt = outTxt + "\n\n"+thisCol
            try:
                outTxt = outTxt + "\n"+"Quartiles (including 0): "+str(np.nanpercentile(tempDF[thisCol],[0,25,50,75,100]))
                outTxt = outTxt + "\n"+"Number of NA's: "+str(np.sum(tempDF[thisCol].isnull()))
                outTxt = outTxt + "\n"+"Percent NA's: "+"{:.2%}".format(np.sum(tempDF[thisCol].isnull())/tempDF.shape[0])
            except:
                try:
                    outTxt = outTxt + "\n"+"Min: "+str(np.nanmin(tempDF[thisCol]))
                    outTxt = outTxt + "\n"+"Max: "+str(np.nanmax(tempDF[thisCol]))
                except:
                    pass
    
    for thisSubset in subSetList:
        if thisSubset!="full":
            subsetDF = pd.read_csv(subsetDict[thisSubset])
            tempDF_subset = tempDF.loc[tempDF.seg_id_nat.isin(subsetDF['seg_id_nat'])]
            nSegs = len(np.unique(tempDF_subset.seg_id_nat))
            nDates = len(np.unique(tempDF_subset.date))
            tempDF_subset.set_index(['date','seg_id_nat'],inplace=True, drop=True)
            tempArr_subset = tempDF_subset.to_xarray().chunk({'seg_id_nat':nSegs,'date':nDates})
            tempArr_subset.to_zarr(os.path.join(outPath,arrayName+"_"+thisSubset+suffix), mode='w')
            
            if tarFiles:
                tardir(os.path.join(outPath,arrayName+"_"+thisSubset+suffix),os.path.join(outPath,arrayName+"_"+thisSubset+suffix+".tar"))
        
    
    nSegs = len(np.unique(tempDF.seg_id_nat))
    nDates = len(np.unique(tempDF.date))
    tempDF.set_index(['date','seg_id_nat'],inplace=True, drop=True)
    tempArr = tempDF.to_xarray().chunk({'seg_id_nat':nSegs,'date':nDates})
    tempArr.to_zarr(os.path.join(outPath,arrayName+"_full"+suffix), mode='w')
    if tarFiles:
            tardir(os.path.join(outPath,arrayName+"_full"+suffix),os.path.join(outPath,arrayName+"_full"+suffix+".tar"))
            
            
    with open("log_%s%s.txt"%(arrayName,suffix),"w+") as f:
        f.write(outTxt)
