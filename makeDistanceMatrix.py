import numpy as np
import pandas as pd
import math
import os
from copy import deepcopy

def makeDistMatrix(fileName, subSetList=[""],subsetDict = {}, outPath="",segsToExclude=[], suffix = ""):
    #read in the csv file
    distDF = pd.read_csv(fileName)
    
    #remove excluded segments
    distDF = distDF[~distDF['from'].isin(segsToExclude)]
    distDF.drop(columns=[str(x) for x in segsToExclude], errors="ignore", inplace=True)
    
    #make the distance matrix files
    rowcolnames = np.array([x for x in distDF.columns[1:]]).astype('<U4')
    
    downstream = distDF.iloc[:,1:].values
    downstream [downstream<0]=math.inf

    upstream =distDF.iloc[:,1:].values
    upstream [upstream>0]=math.inf
    upstream [upstream<0]=-1*upstream [upstream<0]

    updown = distDF.iloc[:,1:].values
    
    np.savez_compressed(fileName.replace(".csv","")+"_full"+suffix, rowcolnames=rowcolnames, downstream=downstream,upstream=upstream,updown=updown)
    
    for thisSubset in subSetList:
        if thisSubset!="full":
            subsetDF = pd.read_csv(subsetDict[thisSubset])
            distDF_subset = distDF.loc[distDF['from'].isin(subsetDF['seg_id_nat'])]
            distDF_subset = distDF_subset[[str(int(x)) for x in subsetDF['seg_id_nat']]]
                                          
            #make the distance matrix files
            rowcolnames_sub = np.array([x for x in distDF_subset.columns]).astype('<U4')

            downstream_sub = deepcopy(distDF_subset.values)
            downstream_sub [downstream_sub<0]=math.inf

            upstream_sub =deepcopy(distDF_subset.values)
            upstream_sub [upstream_sub>0]=math.inf
            upstream_sub [upstream_sub<0]=-1*upstream_sub [upstream_sub<0]

            updown_sub = deepcopy(distDF_subset.values)

            np.savez_compressed(fileName.replace(".csv","")+"_"+thisSubset+suffix, rowcolnames=rowcolnames_sub, downstream=downstream_sub,upstream=upstream_sub,updown=updown_sub)                              
            