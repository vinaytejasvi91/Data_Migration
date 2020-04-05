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
# File:       getTableDetails.py                                                          #
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




class ADLS:

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
    def __init__(self,whatisthis):
        print ''
    def constructHDFSQuery(self,db,table):
        postFix = "_adls"
        adls_root = "'abfss://biddevdatalake01@tcpazbiddc1ntcpdevds1.dfs.core.windows.net/user/hive/warehouse/"

        dbname,tablename = self.splitDBandTable(table)
        adls_location = adls_root + dbname +".db" +"/" + tablename +"/'"
        #print adls_location
        query = "CREATE TABLE %s.%s%s STORED AS PARQUET location %s AS \
                SELECT *  FROM %s.%s" % (dbname,tablename,postFix,adls_location,db,table)
        query = ' '.join(query.split())
        return query

    def constructQuery(self,db,table,adls_location,showCreateTableDetails):
        partition_key = 'effectivestartutcdtprtnkey'
        postFix = "_adls"
        query = "CREATE TABLE %s.%s%s PARTITIONED BY (%s) STORED AS PARQUET location %s AS \
                SELECT * , cast(replace(to_date(effectivestartutcdttm),'-','') as int) %s \
                FROM %s.%s" % (db,table,postFix,partition_key,adls_location,partition_key,db,table)
        query = ' '.join(query.split())
        postFix = "_adls_backup"
        query_backup = "CREATE TABLE %s.%s%s PARTITIONED BY (%s) STORED AS PARQUET location %s AS \
                SELECT * , cast(replace(to_date(effectivestartutcdttm),'-','') as int) %s \
                FROM %s.%s" % (db,table,postFix,partition_key,adls_location,partition_key,db,table)
        query_backup = ' '.join(query_backup.split())
        return query,query_backup
        
    def constructADLSKuduTable(self,db,table,showCreateTableDetails,showRowCount):      
        #print 'Inside constructADLSKuduTable'
        dbName,tableName,adls_location = self.constructLocation(db,table)
        adls_query,adls_backup_query = self.constructQuery(dbName,tableName,adls_location,showCreateTableDetails)
        return adls_query,adls_backup_query
         
    def constructADLSHDFSTable(self,db,every_table,showCreateTableDetails,showRowCount):
        #print 'Inside constructADLSHDFSTable'
        query = self.constructHDFSQuery(db,every_table)
        return query

    def constructLocation(self,db,tableName):
        #print 'Inside constructLocation'
        adls_root = "'abfss://biddevdatalake01@tcpazbiddc1ntcpdevds1.dfs.core.windows.net/app_root/bidh/data/"
        dbName,tableName,env,ssu,source = self.getDetails(tableName)
        adls_location = adls_root + env +"/SDS/database/" +ssu +"/"+ source +"/" +dbName +"/" +tableName +"/'"
        #print adls_location
        return dbName,tableName,adls_location
    def getDetails(self,table):
        #print 'Inside getDetails'
        dbname,tablename = self.splitDBandTable(table)
        #print 'Hello 33'
        env,ssu,source = self.splitEnvSSUSource(dbname)
        return dbname,tablename,env,ssu,source

    def splitDBandTable(self,table):
        #print 'Inside splitDBandTable'
        #print table
        parts = table.split(".")
        #print parts
        if (len(parts[1]) > 0):
            return parts[0], parts[1]
        else:
            return "",""

    def splitEnvSSUSource(self,dbname):
        #print 'splitEnvSSUSource'
        env_list = ['az','svt','dev','cit','e01','e02','e03','e04','e05','e06','e07','e08','e09','e10','e11']
        ssu_list = ['retail','wholesale','corporate']
        #print dbname
        parts = dbname.split("_")
        
        #print parts
        sourceSystem = parts[3]
        #print 'Source system' +sourceSystem
        if (parts[0] in env_list): 
            env = parts[0].upper()
            #print env

        if (parts[1].endswith('r')): 
            ssu = "retail"
        if (parts[1].endswith('c')): 
            ssu = "corporate"
        if (parts[1].endswith('w')): 
            ssu = "wholesale"
        if (parts[1].endswith('t')): 
            ssu = "transient"
        if (parts[1].endswith('e')): 
            ssu = "enterprise"
        #print env,ssu,sourceSystem

        return env, ssu, sourceSystem