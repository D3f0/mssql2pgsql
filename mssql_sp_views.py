#!/usr/bin/env python
# encoding: utf-8
'''
Created on 17/08/2010

Dumpeo de objetos de:
http://technet.microsoft.com/es-es/library/ms345443.aspx

@author: defo
'''


TIPOS=''' P, TR, FN, U, D, V, S, K'''
import sys
from config import mssql_conf
from pymssql import connect
#mssql_conf['host'] = '192.168.56.3'
try:
    conexion = connect(**mssql_conf)
except:
    print "error"
    sys.exit()



def nombre_vistas(conexion = conexion):
    '''
    Retorna los nombres de los procedimientos almacenados
    @param conexion: pymssql.cursor instance
    '''
    cur = conexion.cursor()
    cur.execute("SELECT name FROM sysobjects "
                "WHERE type='v' AND name NOT LIKE '%[_]%' "
                "AND name NOT LIKE 'sys%'")
    result = cur.fetchall()
    cur.close()
    return [ t[0] for t in result ]

def nombres_stored_procs(conexion = conexion):
    '''
    Retorna los nombres de los procedimientos almacenados
    @param conexion: pymssql.cursor instance
    '''
    cur = conexion.cursor()
    cur.execute("SELECT name FROM sysobjects "
                "WHERE type='fn'")
    result = cur.fetchall()
    cur.close()
    return [ t[0] for t in result ]
    

def definicion_elemento(nombre, unir = False, conexion = conexion):
    cursor = conexion.cursor()
    cursor.execute("EXEC sp_helptext N'%s'" % nombre)
    result = [ t[0].strip() for t in cursor.fetchall() ]
    if unir:
        result = '\n'.join(result)
    cursor.close()
    return result


VISTAS = dict([(nombre, definicion_elemento(nombre)) for nombre in nombre_vistas()])
STORED_PROCS = dict([(nombre, definicion_elemento(nombre)) for nombre in nombres_stored_procs()])


if __name__ == "__main__":
    print "Las vistas son:\n\t*", '\n\t* '.join(nombre_vistas())
    print "Los SP son:\n\t*", '\n\t* '.join(nombres_stored_procs())
    LINEA = '#' * 60
    print LINEA
    for nombre in nombre_vistas():
        print definicion_elemento(nombre, unir = True)
    print LINEA
    for nombre in nombres_stored_procs():
        print definicion_elemento(nombre, unir = True)

#cursor.execute('select distinct(type) from sysobjects')
#print '\n'.join([ l[0].strip() for l in cursor.fetchall() ]) 
