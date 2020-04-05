#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
# ###################################################################################
# Copyright © 2019 by Telstra Corporation, All rights reserved                      #
#                                                                                   #
# This software is proprietary to and embodies the confidential technology          #
# of Telstra Corporation. Possession, use, or copying of this software and media is #
# authorized only pursuant to a valid written license from Telstra or               #
# an authorized sublicensor.                                                        #
#                                                                                   #
# File:       impalaDAO.py                                                          #
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

#Import All required libraries
import logging
import os
import getpass

dblistquery = "show databases"
shtable = "show create table"
stats = "show table stats "

from com.telstra.bi2020.dao.ImpalaDAO import ImpalaDAO
from com.telstra.bi2020.dao.Queries import Queries
from com.telstra.bi2020.drivers.getTableDetails import getTableDetails
from com.telstra.bi2020.putils.putil import createFolder
from com.telstra.bi2020.putils.putil import writeFile
from com.telstra.bi2020.ADLS.ADLSTable import ADLS

class listTables:
    
    '''
    /******************************************************************************
    * FUNCTION
    *   __init__
    *
    * DESCRIPTION
    *   This is a constructor, which loads all the database and logging information
    *
    *****************************************************************************/
    '''
    def __init__(self):
        print ''

    def cleanupTableNames(self,tableList):

        ext_table_start = 'Query: show tables'
        ext_table_end = 'Fetched'
        output = tableList
        tlist = str((output[output.find(ext_table_start)+len(ext_table_start):output.rfind(ext_table_end)]).strip())
        return tlist

    def validTable(self,tableName):
        parts = tableName.split(".")
        if (len(parts[1]) > 0):
            return True
        else:
            return False    
    '''
   /******************************************************************************
    * FUNCTION
    *   __init__
    *
    * DESCRIPTION
    *   This is a constructor, which drops all the database and logging information
    *
    *****************************************************************************/
    '''
    def createDropTableQuery(self,tableName):
        dropQuery= "drop table if exists %s " %(tableName)
        return dropQuery
    
    '''
   /******************************************************************************
    * FUNCTION
    *   __init__
    *
    * DESCRIPTION
    *   This is a constructor, which rename all the tables and logging information
    *
    *****************************************************************************/
    '''
    def renameTableQuery(self,tableName):
        adlsTableName = tableName + "_adls"
        renameQuery= "ALTER TABLE %s SET TBLPROPERTIES('EXTERNAL'='TRUE'); \n \
        alter table %s rename to %s; \n \
        ALTER TABLE %s SET TBLPROPERTIES('EXTERNAL'='FALSE');" %(adlsTableName,adlsTableName,tableName,tableName)
        renameQuery = ' '.join(renameQuery.split())
        return renameQuery
    
    '''
    

    /******************************************************************************
    * FUNCTION
    *   __init__
    *
    * DESCRIPTION
    *   This is a constructor, which loads all the database and logging information
    *
    *****************************************************************************/
    '''
    def getTables(self,databases):
        
        dao = ImpalaDAO('Migration')
        conn_object = dao.getConString()
        qs = Queries('TBstart')
        get_query = qs.getQuery('TABLE')
        table = getTableDetails('TABLEDETAILS')
        adls = ADLS('ADLS Object')
        totalCountHDFSTables = 0
        totalCountKUDUTables = 0
        totalcountExternalTables = 0
        log = "Total Number of Filtered(_kmp,_hist,_lk) Databases = %d"  % (len(databases))
        logging.info(log)
        print log
        dbCount = 0
        for db in databases:
            impala_query = conn_object + "'use " +db +';' +get_query +"'"
            tablelist = dao.getResults(impala_query)
            tlist = self.cleanupTableNames(tablelist)
            tcount = 0
            countHDFSTables = 0
            countKuduTables = 0
            countExternalTable = 0
            tablelist = list(db + "." + x for x in tlist.split('\n'))
            table_count = len(tablelist)
            dbCount += 1
            username = getpass.getuser()
            writePath = "/home/%s/mig/com/telstra/bi2020/destination" %(username)
            writePath = writePath + '/' + db +'/' 
            #print writePath
            createFolder(writePath)
            print ''
            print "The datbase " +db +" has " +str(table_count) +" tables"
            if table_count> 0:
                              
                filteredTables = [tbl for tbl in tablelist if "_vw" not in tbl]
                print 'Total Number of Filtered(not _VW) Tables = ' +str(len(filteredTables))
                for every_table in filteredTables:
                    print 'Processing table ' +every_table
                    if self.validTable(every_table):
                        showCreateTableDetails, showTableStatsDetails, showRowCount = table.getTblDetails(every_table)
                    
                        tcount = tcount + 1
                        if "CREATE EXTERNAL TABLE" in showCreateTableDetails:
                            countExternalTable += 1
                        if "STORED AS KUDU" in showCreateTableDetails:
                            countKuduTables += 1
                            fileName_createshowtable =  every_table + '.kudu'  
                            fileName_showrowcount = every_table + '.' + str(showRowCount) + '.kudu' +'.showrowcount'   
                            adls_query, adls_backup_query = adls.constructADLSKuduTable(db,every_table,showCreateTableDetails,showRowCount)     
                            filename_adls = every_table + '.kudu.adls' 
                            writeFile(writePath,filename_adls,adls_query)
                            filename_adls = every_table + '.kudu.adls.backup' 
                            writeFile(writePath,filename_adls,adls_backup_query)
                        else:
                            countHDFSTables += 1
                        
                            fileName_createshowtable =  every_table + '.hdfs'
                            fileName_showtablestats = every_table + '.hdfs' +'.showstats'
                            fileName_showrowcount = every_table + '.' + str(showRowCount)+'.hdfs' +'.showrowcount'
                            adls_hdfs_query = adls.constructADLSHDFSTable(db,every_table,showCreateTableDetails,showRowCount)      
                            filename_hdfs_parquet = every_table + '.hdfs.adls' 
                            writeFile(writePath,filename_hdfs_parquet,adls_hdfs_query)

                            #print('hello5')   
                        #print('hello6')  
                        writeFile(writePath, fileName_createshowtable,showCreateTableDetails)
                        writeFile(writePath, fileName_showrowcount,showRowCount)
                        #print('hello6a')  
                        #print showTableStatsDetails
                        if  (len(showTableStatsDetails) > 0 ):
                            writeFile(writePath, fileName_showtablestats, showTableStatsDetails)
                        #print('hello 7')
                        
                        fileName_dropQuery = every_table + ".drop.sql"
                        dropQuery = self.createDropTableQuery(every_table)
                        writeFile(writePath,fileName_dropQuery,dropQuery)
                        
                        renameQuery = self.renameTableQuery(every_table)
                        fileName_renameQuery = every_table +".rename.sql"
                        writeFile(writePath,fileName_renameQuery,renameQuery)

                print 'Processed Tables Count = ' + str(tcount)
            else:
                print "The datbase " +db +" has " +str(table_count) +" tables (No Processing) "

            writePath = ''      
            log = "Database %s dbCount %d External Tales = %d HDFS Tables = %d Kudu Tables = %d" %(db,dbCount,countExternalTable,countHDFSTables,countKuduTables)    
            logging.info(log)  
            print 'Database ' +db + ' , dbCount = ' + str(dbCount) +' External tables = ' +str(countExternalTable) +' , HDFS Tables = '+str(countHDFSTables) +' , KUDU Tables = '+str(countKuduTables)
            totalCountHDFSTables += countHDFSTables
            totalCountKUDUTables += countKuduTables
            totalcountExternalTables += countExternalTable
        
        print totalCountHDFSTables, totalCountKUDUTables,totalcountExternalTables

        finalResult = "Total Number of HDFS tables = %d \n Total Number of Kudu tables = %d \n Total number of External Tables %d"  % (totalCountHDFSTables,totalCountKUDUTables,totalcountExternalTables)
        resultPath = "../"
        filenameResult = "finalResult.txt"
        writeFile(resultPath,filenameResult,finalResult)     
        logging.info(finalResult)  