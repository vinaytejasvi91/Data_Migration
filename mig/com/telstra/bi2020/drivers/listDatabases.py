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


dblistquery = "show databases"
shtable = "show create table"
stats = "show table stats "

from com.telstra.bi2020.dao.ImpalaDAO import ImpalaDAO
from com.telstra.bi2020.dao.Queries import Queries


class listDatabases:
    
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
    def getDatabases(self):
        dao = ImpalaDAO('Migration')
        conn_object = dao.getConString()
        qs = Queries('DBstart')
        get_query = qs.getQuery('DB');
        impala_query = conn_object + "'" +get_query +"'"
        results = dao.getResults(impala_query)
        return results
