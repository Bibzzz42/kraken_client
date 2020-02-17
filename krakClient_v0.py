# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import argparse
import requests
import csv
import time

import pandas as pd
import datetime as dt

defaultStoragePath=''

def downloadKrakenData(pair='xbtusd', since='0', saveIntermit=True, storagePath=defaultStoragePath,
                       saveResList=[], cautiousMode=True):
    
    req='https://api.kraken.com/0/public/Trades?pair='+pair+'&since='+since
    print(req)
    
    time.sleep(1)
    
    r=requests.get(req)
    print(r.json().keys())
    
    errors=r.json()['error']
    if len(errors)>0:
        strToTxt(storagePath+'kraken_err_'+pair+'_'+since, errors)
        return errors
    
    result=r.json()['result']
    
    
    timeSerie=result[list(result.keys())[0]]
    last=result['last']
    tsCols=['price','volume','time','buy_sell','market_limit','miscellaneous']
    
    
    t=krakenTsToPyFloatFormat(last)
    print(dt.datetime.fromtimestamp(t))
    
    
    if saveIntermit:        
        saveResList=timeSerie
        listToCsv(saveResList, tsCols, storagePath+'kraken_res_'+pair+'_'+since+'_'+last)
    
        if len(timeSerie)>10:
            if cautiousMode:
                strToTxt(pair, last)
            downloadKrakenData(pair, last, True, storagePath)
        else:
            strToTxt(pair, last)
    
    else:
        saveResList+=timeSerie
        
        if len(timeSerie)<10:
            listToCsv(saveResList, tsCols, storagePath+'kraken_res_'+pair)
            strToTxt(pair, last)

        else:
            downloadKrakenData(pair, last, False, storagePath, saveResList)            
    
    return

def strToTxt(fileName, fileContent):
    textFile = open(fileName+'.txt', 'w')
    textFile.write(fileContent)
    textFile.close()
    return

def getLast(pair):
    textFile = open(pair+'.txt', 'r')
    content=textFile.read()
    textFile.close()
    return content

def listToCsv(dTmp, cols, filePath):
    try:
        with open(filePath+'.csv', 'w', newline='') as csvFile:
            writer = csv.writer(csvFile, delimiter=',')
            writer.writerow(cols)
            writer.writerows(dTmp)
        csvFile.close()
        return
                
    except IOError:
        print('I/O error')
        return
        
    
def pyTsToKrakenStrFormat(ts):
    #total len of 19, from prior studies
    v=str(ts).replace('.','')
    while len(v)<19:
        v+='0'
    return v

def krakenTsToPyFloatFormat(timestamp):
    #total int len of 10, from prior studies
    v=float(timestamp[:10]+'.'+timestamp[10:])
    return v
    
if __name__=='__main__':
#    testwrite()

    parser = argparse.ArgumentParser(description='Get data from kraken.')
    parser.add_argument('-routine', metavar='routine', type=str,
                        help='Available routines: downloadKrakenData.')
    
    args = parser.parse_args()
    
    pair=''
    since=''
    path=''
    
    if args.routine=='downloadKrakenData':
        pStr='Pair?\ne.g. xbtusd\n'
        
        pair=input(pStr)
        
        
        sStr='int timestamp since date with len=19, '
        sStr+='e.g. 1559347200000000000\n0 to start from first trade\n'
        sStr+='-1 to update from last downloaded trade.\n'
        
        since=input(sStr)
        
        if since=='-1':
            since=getLast(pair)
            
            
        storeStr='Path for storage\n'
        storeStr+='-1 for default path.\n'
        
        path=input(storeStr)
        
        if path=='-1':
            path=defaultStoragePath
        #downloadKrakenData(pair, since, True)

    downloadKrakenData(pair, since, True, storagePath=path)
    
