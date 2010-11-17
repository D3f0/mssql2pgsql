'''
Created on 26/07/2010

A partir de una Query buscamos los campos en la base de datos oroginal.

@author: defo
'''

import sys
import re
import cPickle as pickle
TABLAS = None

    
from pymssql import connect
from pprint import pprint

from connections import vitualbox as conexion

# http://forums.aspfree.com/sql-development-6/how-get-table-definition-from-sql-server-tables-264622.html
SQL = '''
SELECT a.[name] as 'Table',
  b.[name] as 'Column',
  c.[name] as 'Datatype',
  b.[length] as 'Length',
  CASE
   WHEN b.[cdefault] > 0 THEN d.[text]
   ELSE NULL
  END as 'Default',
  CASE
   WHEN b.[isnullable] = 0 THEN 'No'
   ELSE 'Yes'
  END as 'Nullable'
FROM  sysobjects a
INNER JOIN syscolumns b
ON  a.[id] = b.[id]
INNER JOIN systypes c
ON  b.[xtype] = c.[xtype]
LEFT JOIN syscomments d
ON  b.[cdefault] = d.[id]
WHERE a.[xtype] = 'u'
-- 'u' for user tables, 'v' for views.
--and a.[name]='table name'
AND  a.[name] <> 'dtproperties'
ORDER BY a.[name],b.[colorder]
'''

BAN_TABLAS = (
    re.compile('^conflict', re.IGNORECASE | re.UNICODE),
    re.compile('^MS', re.IGNORECASE | re.UNICODE),
    re.compile('^merge', re.IGNORECASE | re.UNICODE),
    re.compile('^BATCH_', re.IGNORECASE | re.UNICODE),
    re.compile('^sys', re.IGNORECASE | re.UNICODE),
    'pruebarepl',
    # Esta es la diferencia que sacamos los scripts
    'Q_NaturalezasCamuzzi', 
    'Q_ApropiacionesCamuzzi', 
    'Tmp_LineasAT', 
    'tmp_planimetrias', 
    'Hoja1$',
    'asistencianovviejo',
    'asistenciasectores',
    'Q_asistenciameslaboral',
    'mntclaseplanillacampos',
    'mntclaseplanillas',
    'mntequipomedicioneshis',
) 

cursor_cuenta = None
    
cache_counts = {}

def es_tabla_util(nombre):
    global cursor_cuenta
    if conexion and not cursor_cuenta:
        cursor_cuenta = conexion.cursor()
    for ban in BAN_TABLAS:
        if isinstance(ban,  basestring):
            if nombre.lower() == ban.lower():
                return False
        else:
            match = ban.search(nombre)
            
            if match != None:
                print "Match en", nombre
                return False
            
    if not nombre in cache_counts:
        cursor_cuenta.execute('select count(*) from %s' % nombre)
        cant = cursor_cuenta.fetchone()[0]
        cant = int(cant)
        cache_counts[nombre] = cant
    else:
        cant = cache_counts[nombre] 
    
    if not cant:
        return False
                     
    return True
    
def main(argv = sys.argv):
    if not conexion:
        return
    cursor = conexion.cursor()
    cursor.execute(SQL)
    datos = cursor.fetchall()
    
    tablas = {}
    #tablas.setdefault([])
    for tabla, columna, tipo, longitud, default, null in datos:
        if not es_tabla_util(tabla):
            continue
        null = null.lower() == 'yes'
        item = dict(nombre = columna, tipo=tipo, longitud=longitud, 
                    default = default, null = null)
        if not tabla in tablas:
            tablas[tabla] = [item, ]
        else:
            tablas[tabla].append(item)
    
    TABLAS = tablas
    pickle.dump(TABLAS, open('tablas.pickle', 'w'))
    pprint(tablas)
    print(len(tablas))
    for tabla, cant in cache_counts.iteritems():
        if cant == 0:
            print tabla    


if __name__ == "__main__":
    sys.exit(main())
else:
    
    with file('tablas.pickle') as f:
        TABLAS = pickle.load(f)
    sys.stderr.write("Tablas recuperadas de persistencia\n")
    
        
        
