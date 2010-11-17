#!/usr/bin/env python
# coding: utf-8
'''
Created on 26/07/2010

Conversión del esquema de SQL Server a Postgres

@author: defo
'''

from mssql_schema import TABLAS
import re
from config import pg_dsn
from itertools import izip
import sys

#===============================================================================
# Los tipos que tienen una cadena de reemplazo decimal aceptan
#===============================================================================
TIPOS_MSSQL_PG = {
    'ntext': 'text',
    'text': 'text',
    # No me sirve el booleano
    'bit': 'boolean', 
    'nvarchar': 'varchar(%d)',
    'varchar': 'varchar(%d)', # Para afectarlo en la conversión
    'uniqueidentifier': None, #'uuid',
    'sysname': None,
    # file:///usr/share/doc/postgresql-doc-8.4/html/datatype-datetime.html
    'smalldatetime': 'timestamp',
    'datetime': 'timestamp',
    # Binarios
    'image': 'bytea',
    'varbinary': 'bytea',
    # enteros
    'int': 'int', 
    'tinyint': 'smallint',
    'smallint': 'smallint',
    'money': 'numeric(19,4)',
    
}


def columna_util(nombre, tipo, **kwargs):
    '''
    Define si una columna es de interes
    '''
    try:
        if TIPOS_MSSQL_PG[tipo] == None:
            return False
    except:
        pass
    return True

def columnas_utiles(lista_de_columnas):
    ''' '''
    salida = []
    for col_def_dict in lista_de_columnas:
        if columna_util(**col_def_dict):
            salida.append(col_def_dict)
    return salida 

def print_f_io(f):
    ''' Debug, imprime la entrada y salida de una función '''
    def wrapped(*largs, **kwargs):
        print f.func_name, ', '.join(map(unicode, largs)), ', '.join(["%s=%s" % (k, v) for k, v in kwargs.iteritems()])
        retval = f(*largs, **kwargs)
        print retval
        return retval
    return wrapped


def tipo_mssql_a_pgsql(tipo, longitud):
    ''' Cambia el tipo de datos de una columna al equivalente en pgsql '''
    tipo = tipo and tipo.lower()
    if tipo in TIPOS_MSSQL_PG:
        tipo = TIPOS_MSSQL_PG[tipo]
        if tipo is None: return
        if tipo.count('%') and longitud:
            # Con longitud
            if tipo.count('varchar'):
                longitud = int(longitud) * 120 / 100 # Sumamos un 10%
                print "Aumento de capacidad de varchar al 20%" 
            return tipo % int(longitud)
        else:
            # Sin longtud
            return tipo
    return tipo + (longitud and '(%s)' % longitud or '')
    

NUMERO = re.compile('^\d+$')

#@print_f_io
def sql_crear_fila(nombre, tipo, longitud, default, null, **kwargs):
    if NUMERO.match(nombre):
        nombre = '"%s"' % nombre
    tipo_longitud = tipo_mssql_a_pgsql(tipo, longitud)
    if not tipo_longitud:
        return
    return ' '.join([nombre, tipo_longitud, null and 'NULL' or 'NOT NULL'])

def sql_crear_tabla(nombre, lista_de_columnas, pre_drop):
    '''
    Crear las sentencias de creación de una tabla
    @param nombre: Nombre de la tabla
    @param lista_de_columnas: Lista de diccionarios con la definición de la tabla
    @param pre_drop: Eliminar la tabla si existe 
    '''
    salida = ''
    sql_columnas = map(lambda d: sql_crear_fila(**d), lista_de_columnas)
    columnas = ',\n\t'.join(filter(bool, sql_columnas))
    if pre_drop:
        salida = 'DROP TABLE IF EXISTS %s CASCADE;\n' % nombre
     
    salida = ('%sCREATE TABLE %s (\n\t'
              '%s\n' # a
              ');\n') % (salida, nombre, columnas) 
    return salida

def xgenerar_sql_esquema(tablas = TABLAS, pre_drop = True):
    '''
    Genrador de la definición de un conjunto de tablas
    @param pre_drop: Elimina la tabla si existe antes de la sentencia CREATE
    @param tablas: Un diccionario de nombre de tablas contra lista de columnas 
    como diccionarios. Por defecto toma el del esquema de mssql.
    '''
    
    for nombre, columnas in tablas.iteritems():
        yield sql_crear_tabla(nombre, columnas, pre_drop)
        
def generar_sql_esquema(tablas = TABLAS, pre_drop = True):
    '''
    Genra la definición de un conjunto de tablas
    @param pre_drop: Elimina la tabla si existe antes de la sentencia CREATE
    @param tablas: Un diccionario de nombre de tablas contra lista de columnas 
    como diccionarios. Por defecto toma el del esquema de mssql.
    '''
    return '\n'.join([sql for sql in xgenerar_sql_esquema(tablas, pre_drop)])



def generar_tablas(dsn_destino = pg_dsn):
    '''
    '''
    from psycopg2 import connect
    conexion = connect(dsn_destino)
    curs = conexion.cursor()
    nombre_tablas = TABLAS.keys()
    contador = 0
    for tabla, sql in izip(nombre_tablas, xgenerar_sql_esquema()):
        curs.execute(sql)
        conexion.commit()
        print tabla, "OK"
        contador += 1
    return contador
    # Está en trust 

def main(argv = sys.argv):
    print "Generando el esquma en Postgre"
    print generar_tablas()
    
if __name__ == "__main__":
    sys.exit(main())
    