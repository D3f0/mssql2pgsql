# encoding: utf-8
'''
Created on 26/07/2010

Comparación de las tablas de el 32 contra el 30

@author: defo
'''
import sys

from connections import con_30, con_32 # MSSQL Connections


def tablas_conexion(con):
    cursor = con.cursor()
    cursor.execute("select name from sysobjects where type='u'")
    nombre_tablas = [ f[0] for f in cursor.fetchall() ]
    cursor.close()
    return nombre_tablas

tablas_32 = tablas_conexion(con_32)
tablas_30 = tablas_conexion(con_30)

print " las tablas del 32 es ", len(tablas_32)
print " las tablas del 30 es ", len(tablas_30)
talbas_diferencia = [ t for t in tablas_32 if not t in tablas_30 and not t.startswith('conflict')]
print talbas_diferencia
#print '\n'.join(talbas_diferencia)

sys.exit()
'''
Created on 26/07/2010

@author: defo
'''
'''
Created on 22/07/2010

@author: defo
'''
import pymssql
import psycopg2
import sys

from connections import sqlserver

def _busqueda_tablas():
    ms_cur = sqlserver.cursor()
    ms_cur.execute("SELECT name FROM sysobjects WHERE type='u'"
                   "AND NOT name LIKE 'conflict%' "
                   "AND NOT name LIKE 'MS%' "
                   " ORDER BY name"
                   )
    return [ d['name'] for d in ms_cur.fetchall_asdict() ]

TABLAS_SQL_SERVER = _busqueda_tablas()

#curs = sqlserver.cursor()
#curs.execute("SELECT TOP 1 * FROM %s" % TABLAS_SQL_SERVER[0])
#print curs.fetchone()

prows = lambda tl: "\n".join([",".join(map(unicode, x)) for x in tl ])

curs = sqlserver.cursor()
curs.execute("SELECT  COUNT(*) FROM sysusers")
filas = curs.fetchall()
print prows(filas)
print len(filas)



sys.exit()
from PyQt4.Qt import *
app = QApplication(sys.argv)
win = QWidget()
layout = QVBoxLayout()
lista = QListWidget()
layout.addWidget(lista)

lista.addItems(["%d %s"  % (n, s) for n, s in enumerate(TABLAS_SQL_SERVER)])
win.setLayout(layout)
win.show()
app.exec_()