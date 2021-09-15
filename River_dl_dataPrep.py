# -*- coding: utf-8 -*-
"""
Created on Thu Aug  5 13:17:25 2021

@author: jbarclay
"""

### modifying sciencebase data to be prepped for river-dl
#https://www.sciencebase.gov/catalog/item/5f6a287382ce38aaa2449131
#https://www.sciencebase.gov/catalog/item/5f6a289982ce38aaa2449135

import os
import numpy as np
import pandas as pd
import xarray as xr
import sciencebasepy
import tarfile
import zipfile


#track the files that need to tarred
filesToTar = []

####################################
#download the files from sciencebase
itemDict = {'5f6a289982ce38aaa2449135':['sntemp_inputs_outputs_drb.zip'], '5f6a287382ce38aaa2449131':['temperature_observations_drb.zip','flow_observations_drb.zip']}
sb = sciencebasepy.SbSession()
for item, filenameLst in itemDict.items():
    item_json = sb.get_item(item)
    fileList = sb.get_item_file_info(item_json)
    for filename in filenameLst:
        fileDict = [x for x in fileList if x['name']==filename][0]
        sb.download_file(fileDict['url'], os.path.join(os.getcwd(),filename))
        
        if filename.endswith("zip"):
            with zipfile.ZipFile(os.path.join(os.getcwd(),filename)) as z:
                z.extractall(os.path.join(os.getcwd()))
###################################
## load array identifying segments in the subset
christianaDF = pd.read_csv("ChristianaReaches.csv")


##########################
#SNTemp inputs and outputs
filenameSNTEMP = 'sntemp_inputs_outputs_drb'
sntempDF = pd.read_csv("%s.csv"%(filenameSNTEMP))
sntempDF.date=sntempDF.date.astype('datetime64[ns]')

#convert missing sntemp water temps to na
sntempDF.loc[(sntempDF.seg_tave_water<(-90)),"seg_tave_water"]=np.nan
sntempDF.loc[(sntempDF.seg_tave_water>(50)),"seg_tave_water"]=np.nan

# make the subset sntemp file
sntempDF_subset = sntempDF.loc[sntempDF.seg_id_nat.isin(christianaDF['seg_id_nat'])]
filenameSNTEMP_subset = 'sntemp_inputs_outputs_subset'

#make the arrays for the sntemp files
sntempDF.set_index(['date','seg_id_nat'],inplace=True, drop=True)
sntempArr = sntempDF.to_xarray()
sntempArr.to_zarr(filenameSNTEMP, mode='w')
filesToTar.append(filenameSNTEMP)

#make the arrays for the sntemp subset files
sntempDF_subset.set_index(['date','seg_id_nat'],inplace=True, drop=True)
sntempArr_subset = sntempDF_subset.to_xarray()
sntempArr_subset.to_zarr(filenameSNTEMP_subset, mode='w')
filesToTar.append(filenameSNTEMP_subset)


##################
#flow observations
filenameFlow = 'flow_observations_drb'
flowDF = pd.read_csv("%s.csv"%(filenameFlow))
flowDF = flowDF[['seg_id_nat','date','discharge_cms']]
flowDF.date=flowDF.date.astype('datetime64[ns]')
nSegsFlow = len(np.unique(flowDF.seg_id_nat))
nDatesFlow = len(np.unique(flowDF.date))

##################
#temp observations
filenameTemp = 'temperature_observations_drb'
tempDF = pd.read_csv("%s.csv"%(filenameTemp))
#drop unneeded columns
tempDF = tempDF[['seg_id_nat','date','mean_temp_c']]
tempDF.rename(columns={'mean_temp_c':'temp_c'},inplace=True)
tempDF.date=tempDF.date.astype('datetime64[ns]')
nSegsTemp = len(np.unique(tempDF.seg_id_nat))
nDatesTemp = len(np.unique(tempDF.date))


####################
#combined input file with flow and temp
combinedDF = tempDF.merge(flowDF, how="outer")
nSegsCombined = len(np.unique(tempDF.seg_id_nat))
nDatesCombined = len(np.unique(tempDF.date))

#make a combined array for the subset
subsetDF = combinedDF.loc[combinedDF.seg_id_nat.isin(christianaDF['seg_id_nat'])]
nSegsSubset = len(np.unique(subsetDF.seg_id_nat))
nDatesSubset = len(np.unique(subsetDF.date))

##############
## make arrays from the flow, temp, and combined DF
flowDF.set_index(['seg_id_nat','date'], drop=True, inplace=True)
flowArr = flowDF.to_xarray().chunk({'seg_id_nat':nSegsFlow,'date':nDatesFlow})
flowArr.to_zarr(filenameFlow, mode='w')
filesToTar.append(filenameFlow)




tempDF.set_index(['seg_id_nat','date'], drop=True, inplace=True)
tempArr = tempDF.to_xarray().chunk({'seg_id_nat':nSegsTemp,'date':nDatesTemp})
tempArr.to_zarr(filenameTemp, mode='w')
filesToTar.append(filenameTemp)


combinedDF.set_index(['seg_id_nat','date'], drop=True, inplace=True)
combinedArr = combinedDF.to_xarray().chunk({'seg_id_nat':nSegsCombined,'date':nDatesCombined})
combinedArr.to_zarr('obs_temp_flow_full', mode='w')
filesToTar.append('obs_temp_flow_full')


subsetDF.set_index(['seg_id_nat','date'], drop=True, inplace=True)
subsetArr = subsetDF.to_xarray().chunk({'seg_id_nat':nSegsSubset,'date':nDatesSubset})
subsetArr.to_zarr('obs_temp_flow_subset', mode='w')
filesToTar.append('obs_temp_flow_subset')


def tardir(path, tar_name):
    with tarfile.open(tar_name, "w") as tar_handle:
        for root, dirs, files in os.walk(path):
            for file in files:
                tar_handle.add(os.path.join(root, file))

for thisFile in filesToTar:
    tardir(thisFile,"{}.tar".format(thisFile))