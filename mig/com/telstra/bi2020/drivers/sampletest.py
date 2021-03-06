#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
# ###################################################################################
# Copyright � 2019 by Telstra Corporation, All rights reserved                      #
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


show_create_table = "show create table"
table_stats = "show table stats "
table_rows_count = "select count (*) from "

from com.telstra.bi2020.dao.ImpalaDAO import ImpalaDAO
from com.telstra.bi2020.dao.Queries import Queries


class getTableDetails:
    
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

        print 'inside cleanupTableNames'
        ext_table_start = 'Query: show tables'
        ext_table_end = 'Fetched'
        output = tableList
        tlist = str((output[output.find(ext_table_start)+len(ext_table_start):output.rfind(ext_table_end)]).strip())
        return tlist;

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
    def getTableDetails(self,tableName):

        dao = ImpalaDAO('Migration')
        conn_object = dao.getConString()
        qs = Queries('TBstart')
        get_query = qs.getQuery('TABLE');
    
        showCreateTable();
        showTableStats()
        showRowsCount()

        return;


    def showCreateTable(self,tableName):
        dao = ImpalaDAO('Migration')
        conn_object = dao.getConString()
        qs = Queries('GETTABLEDETAILS')
        get_query = qs.getQuery('CREATE');

        impala_query = conn_object + get_query + tablename

        print impala_query

    def showTableStats(self,tableName):
        dao = ImpalaDAO('Migration')
        conn_object = dao.getConString()
        qs = Queries('GETTABLEDETAILS')
        get_query = qs.getQuery('STATS');

        impala_query = conn_object + get_query + tablename

        print impala_query

    def showRowsCount(self,tableName):
        dao = ImpalaDAO('Migration')
        conn_object = dao.getConString()
        qs = Queries('GETTABLEDETAILS')
        get_query = qs.getQuery('COUNT');

        impala_query = conn_object + get_query + tablename

        print impala_query
    

'''
    def cleanupTableNames(tableList):

        ext_table_start = 'Query: show tables'
        ext_table_end = 'Fetched'
        print 'inside cleanupTableNames'
        output = tableList
        tlist = str((output[output.find(ext_table_start)+len(ext_table_start):output.rfind(ext_table_end)]).strip())
        return tlist;
   for table in tablelist:
       if table_count > 0:
           shtable = "show create table " +table
           stats = "show table stats " + table
           rcount = "select count (*) from " + table


           result_string = connect_string + "'" +shtable +"'"
           status, output = commands.getstatusoutput(result_string)

           ext_show_table_start = shtable
           ext_show_table_end = 'Fetched'
           showcreatetable = str((output[output.find(ext_show_table_start)+len(ext_show_table_start):output.rfind(ext_show_table_end)]).strip())
           print "show create table: " +showcreatetable

           ext_show_table_stats_start = stats;
           ext_show_table_ends = 'Fetched'

           result_string = connect_string + "'" +stats +"'"
           status, output = commands.getstatusoutput(result_string)
           showtablestats = str((output[output.find(ext_show_table_stats_start)+len(ext_show_table_stats_start):output.rfind(ext_show_table_ends)]).strip())
           print "show table stats : " +showtablestats

           ext_show_table_count_start = rcount
           ext_show_table_count_end = 'Fetched'

           result_string = connect_string + "'" +rcount +"'"
           status, output = commands.getstatusoutput(result_string)
           showrowcount = str((output[output.find(ext_show_table_count_start)+len(ext_show_table_count_start):output.rfind(ext_show_table_count_end)]).strip())
           print "show row count: " +showrowcount

'''
