#!/usr/bin/env python
# coding: utf-8
'''
Created on 17/08/2010

@author: defo
'''

import pymssql
import psycopg2 

recrodset_lista = lambda r: [ elemento[0] for elemento in r ]
cursor_lista = lambda cursor: [ ','.join(map(unicode, tupla)) for tupla 
                               in cursor.fetchall() ]

class DataBase(object):
    def __init__(self, nombre, **opts):
        self.nombre = nombre
        self._connect(**opts)
        
    def _connect(self):
        raise NotImplementedError

class MSSQL(DataBase):
    def _connect(self, **opts):
        self.conexion = pymssql.connect(**opts)
        self._tablas = []
        self._vistas = []
        self._stored_procs = []
        self._definiciones = dict(
                                 vistas = [], 
                                 tablas = [], 
                                 stored_procs = []
                                 )
    
    @property
    def tablas(self):
        if not self._tablas:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT name FROM sysobjects WHERE type='u' ORDER BY name")
            self._tablas = cursor_lista(cursor)
            cursor.close()
        return self._tablas
    
    @property
    def vistas(self):
        if not self._vistas:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT name FROM sysobjects WHERE type='v' "
                           "AND name not like 'ctsv[_]%' AND name not like 'tsvw[_]%' "
                           "AND name not like 'MSmerge_contents_%' " # Tablas de merge
                           "ORDER BY name")
            self._vistas = cursor_lista(cursor)
            cursor.close()
        return self._vistas
    
    @property
    def sotred_procs(self):
        if not self._stored_procs:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT name FROM sysobjects WHERE type='fn' "
                           "ORDER BY name")
            self._stored_procs = cursor_lista(cursor)
            cursor.close()
        return self._stored_procs
    
    def definicion_vista(self, nombre):
        if not nombre in self._definiciones['vistas']:
            cursor = self.conexion.cursor()
            cursor.execute("EXEC sp_helptext N'%s'" % nombre)
            self._definiciones['vistas'] = [ t[0].strip() for t in cursor.fetchall() ]
            cursor.close()
        return self._definiciones['vistas']
    
    def definicion_stored_proc(self, nombre):
        if not nombre in self._definiciones['stored_procs']:
            cursor = self.conexion.cursor()
            cursor.execute("EXEC sp_helptext N'%s'" % nombre)
            self._definiciones['stored_procs'] = [ t[0].strip() for t in cursor.fetchall() ]
            cursor.close()
        return self._definiciones['stored_procs']
        
class PgSQL(DataBase):
    def __init__(self, nombre, **kwargs):
        pass
    
    
            

class Migracion(object):
    '''
    Encapsula la 
    '''
    
    def __init__(self, origen, destino):
        self.origen, self.destino = origen, destino
    
    
    def migrar_datos(self):
        pass
    
    def migrar_estrctura(self):
        pass
    
    
    def migrar(self):
        pass


if __name__ == "__main__":
    origen = MSSQL(nombre = "SQL Server", database = "TP_Admin", host = "192.168.1.30", user = 'sa', password = 'sa')
    
    for nombre, datos in zip(['tablas', 'vistas', 'procedimientos'], [origen.tablas, origen.vistas, origen.sotred_procs]):
        n = 0
        for n, elem in enumerate(datos):
            print "* %s %d: %s" % (nombre, n, elem)
        print "Total %s: %d" % (nombre, n)
        
        print origen.definicion_vista(origen.vistas[4])