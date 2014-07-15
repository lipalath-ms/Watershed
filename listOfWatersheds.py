import requests
import json
import zipfile
import os
import progressbar
import time
import sys

def   listWatersheds():

    '''
    listWatersheds function contacts GSTORE and returns a list of watersheds.
    '''
    
    wIndex = 1
    collectionNames = []
    collectionIds = []
   
    rWatersheds = requests.get('http://gstore.unm.edu/apps/epscor/search/collections.json?version=3')
    rData = rWatersheds.json()
    watershedResults = rData['results']

    #Get the length of Watershed list
    countOfWatersheds = rData['subtotal']

    #displays the name of all watersheds
    print "\n\nName of watersheds available:\n\n"
    for wResult in watershedResults:
        print wIndex, ":", wResult['name']
        wIndex += 1
        collectionNames.append(wResult['name']) 
        collectionIds.append(wResult['uuid']) 
    return countOfWatersheds, collectionNames, collectionIds

def getWatershedDetails(userWatershedChoice, wDetails):
    
    '''
    getWatershedDetails function finds out the uid of the selected watershed
    '''

    nameOfWatershed = wDetails[1][userWatershedChoice - 1]
    uidOfWatershed = wDetails[2][userWatershedChoice - 1]
    return nameOfWatershed, uidOfWatershed

def   listDatasets(nameOfWatershed, uidOfWatershed):

    '''
    listDatasets function contacts GSTORE and returns a list of datasets.
    '''

    rIndex = 1
    datasetNames = []
    datasetIds = []
        
    rDatasets = requests.get('http://gstore.unm.edu/apps/epscor/search/collection/%s/datasets.json?version=3' %uidOfWatershed) 
    rrData = rDatasets.json()
    dataResults = rrData['results']
    
    #Get the length of Dataset list
    countOfDatasets = rrData['subtotal']

    #displays the name of all datasets
    print "\n\nDatasets available for %s: " %nameOfWatershed, "\n\n"
    for dResult in dataResults:
        print rIndex, ":", dResult['name']
        rIndex += 1
        datasetNames.append(dResult['name']) 
        datasetIds.append(dResult['uuid']) 
    return countOfDatasets, datasetNames, datasetIds  

def selectADataset(dDetails):

    '''
    selectADataset function performs various tasks based on user choice
    '''

    userDatasetChoice = input("\n\nSelect an option: ")
    if not 1 <= userDatasetChoice <= dDetails[0]:
        print "Invalid option. Please try again"
        selectADataset(dDetails)
    else:
        return userDatasetChoice

def getDatasetDetails(userDatasetChoice, dDetails):
    
    '''
    getWatershedDetails function finds out the uid of the selected watershed
    '''

    nameOfDataset = dDetails[1][userDatasetChoice - 1]
    uidOfDataset = dDetails[2][userDatasetChoice - 1]
    return nameOfDataset, uidOfDataset


def downloadDataset(nameOfDataset, uidOfDataset):

    '''
    downloadDataset function contacts GSTORE and downloads dataset selected by the user
    '''

    progress = progressbar.ProgressBar()
    url = "http://gstore.unm.edu/apps/epscor/datasets/%s/%s.original.tif" %(uidOfDataset, nameOfDataset)
    response = requests.head(url)
    print "Content length of %s: " %nameOfDataset, response.headers['content-length']
    print "\nDownloading ..."
    rDownload = requests.get(url)
    for i in progress(range(100)): 
        with open("%s.zip" %nameOfDataset, "wb") as code:	  
            code.write(rDownload.content)
        time.sleep(0.01)
    print "\nDownloading completed\n"  
    zip = zipfile.ZipFile(r'%s.zip' %nameOfDataset)
    zip.extractall(r'%s' %nameOfDataset)

    #List the name of files available from the downloaded file
    print "Files available are:\n"
    dirList = os.listdir(nameOfDataset)
    for fname in dirList:
        print fname  
    return

def multipleSelect(watershedDetails):
  
    '''
    multipleSelect function asks user for more selects
    '''

    while True:
        userMDChoice = raw_input("\nDo you want to download more datasets? (Yes/No):\t")
        if userMDChoice == "Yes":
            dsteps(watershedDetails)
        else:
            userMWChoice = raw_input("\nDo you want to select another watershed? (Yes/No):\t")
            if userMWChoice == "Yes":
                steps()
            else:
                sys.exit()   

def steps():
    wDetails = listWatersheds()
    while True:
        userWatershedChoice = input("\n\nSelect an option: ")
        if 1 <= userWatershedChoice <= wDetails[0]:
            break
        else:
            print "Invalid option. Please try again."     
    watershedDetails = getWatershedDetails(userWatershedChoice, wDetails)
    dsteps(watershedDetails)
  
def dsteps(watershedDetails):
    dDetails = listDatasets(watershedDetails[0], watershedDetails[1])
    while True:
        userDatasetChoice = input("\n\nSelect an option: ")
        if 1 <= userDatasetChoice <= dDetails[0]:
            break
        else:
            print "Invalid option. Please try again."     
    datasetDetails = getDatasetDetails(userDatasetChoice, dDetails)
    downloadDataset(datasetDetails[0], datasetDetails[1])
    multipleSelect(watershedDetails)
    
            
if __name__ == "__main__":
    steps()
    
    
    
    
   
        
    
   
    
