#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
# ###################################################################################
# Copyright © 2019 by Telstra Corporation, All rights reserved                      #
#                                                                                   #
# This software is proprietary to and embodies the confidential technology          #
# of Telstra Corporation. Possession, use, or copying of this software and media is #
# authorized only pursuant to a valid written license from Tesltra or               #
# an authorized sublicensor.                                                        #
#                                                                                   #
# File:       Driver.py                                                             #
# Created:    Platform Team Engineers, 17-Oct-2019                                  #
#                                                                                   #
# Modification history:                                                             #
# None                                                                              # 
#                                                                                   #
# <Example>                                                                         #
# Modified:  John   05-May-2020                                                     #
#            Jira Task#: Modification for logging format                            #
#                                                                                   #
#####################################################################################
'''
/******************************************************************************
* FUNCTION
*   getListofdatabases
*
* DESCRIPTION
*   Description goes here
* 
*****************************************************************************/
'''    

import logging
import sys
import time
import traceback
import multiprocessing
import getpass



from com.telstra.bi2020.drivers.listDatabases import listDatabases 
from com.telstra.bi2020.drivers.listTables import listTables 

def getListofdatabases():
    try:
        start = time.time()
        dblist = listDatabases()
        dlist = dblist.getDatabases()
        dlist = cleanupDatabaseNames(dlist);
        return dlist
    except:
        raise

def cleanupDatabaseNames(listofDBs):
    output = listofDBs
    start = 'System database for Impala builtin functions'
    end = 'Fetched'
    dblist = str((output[output.find(start)+len(start):output.rfind(end)]).strip())
    dblist = list(dblist.split("\t"))
    dblist2 = [x.replace('\n', '') for x in dblist]
    del dblist2[-1]
    del dblist2[0]
    del dblist2[0]
    filteredDBS = [db for db in dblist2 if "_hist" in db]
    filteredDBS.append([db for db in dblist2 if "_kmp" in db])
    #filteredDBS.append([db for db in dblist2 if "_lk" in db])
    print len(filteredDBS)
    return filteredDBS

def getListofTables(databases):
    try:
        start = time.time()
        dblist = listTables()
        dlist = dblist.getTables(databases)
        return dlist
    except:
        raise


def main():
    username = getpass.getuser()
    filepath = "/home/%s/mig/com/telstra/bi2020/log/migration.log" %(username)
    logging.basicConfig(filename=filepath,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

    databases = getListofdatabases();
    tables = getListofTables(databases);
    print databases; 


'''
/******************************************************************************
* FUNCTION
*   main function
*
* DESCRIPTION
*    This is main driver, This drives the whole process. First of it takes the input
*    from user and calls the respective driver
* 
*****************************************************************************/
'''    
if __name__ == '__main__':
    try:
        main()
    except:
        sys.exit(1)
