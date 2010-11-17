#!/usr/bin/env python
# coding: utf-8
'''
Created on 17/08/2010

Transformar la definción de 

@author: defo
'''
from config import set_mssql_conf
set_mssql_conf('host', '192.168.56.2')
from mssql_sp_views import VISTAS, STORED_PROCS
for nombre, definicion in VISTAS.iteritems():
    print nombre, definicion