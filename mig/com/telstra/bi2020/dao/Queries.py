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
import re

#Import All required libraries

dblistquery = "show databases"
show_create_table = "show create table " 
show_stats = "show table stats " 
rows_count = "select count(*) from "
showtable = "show tables;"


class Queries:

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
    def __init__(self,purpose):
        self.purpose = purpose

    def getQuery(self,whichtype):
        if whichtype == 'DB':
            return dblistquery
        if whichtype == 'TABLE':
            return showtable

        if whichtype == 'CREATE':
            return show_create_table

        if whichtype == 'STATS':
            return show_stats

        if whichtype == 'COUNT':
            return rows_count


