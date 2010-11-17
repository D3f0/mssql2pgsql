#!/usr/bin/env python
# coding: utf-8
'''
Created on 18/07/2010

La idea original era indentificar el T-SQL generado por el DUMP de la 
herramienta administrativa de SQL Server y a patir de esta generar las
tablas en postgres utilizando el dirver de conexión con Python llamado PyscoPg2

@author: defo
'''

import re
import codecs

FILENAME = 'script_utf8.sql'

RE_SQL_CREACION_TABLA = re.compile('''
    CREATE\sTABLE\s
        \[dbo\]\.\[
        (?P<tabla>[\d\w\-\_]+)\]\(
            (?P<defincion>[\s\d\w\_\[\]\(\)\,]+)
        \)
    
''', re.VERBOSE | re.UNICODE | re.MULTILINE | re.IGNORECASE)

RE_SQL_DEFINICION_COLUMNA = re.compile('''
    \[(?P<nombre>[\d\w\_\-]+)\]\s*
    \[(?P<tipo>[\d\w\_\-\(\)]+)\]\s*
    (:?\((?P<longitud>\d+)\))*
    (?P<nulidad>(:?NOT)?\sNULL)
''', re.VERBOSE | re.UNICODE)


#===============================================================================
# Mapeos de Tipos de SQL Server a Postgres
#===============================================================================
REDEFINICION_CAMPOS = {
    'nvarchar': 'varchar',
    'uniqueidentifier': 'uuid',
    # file:///usr/share/doc/postgresql-doc-8.4/html/datatype-datetime.html
    'smalldatetime': 'timestamp',
    'datetime': 'timestamp',
    'ntext': 'text',
    'image': 'bytea',
    'tinyint': 'smallint',
    'money': 'numeric(19,4)',
    #'varbinary': 'bytea',
}

TIPOS_UTILIZADOS = set()

NUMERO = re.compile(r'^\d+$')

def tipo_mssql_a_pgsql(tipo, longitud):
    longitud = longitud and '(%s)' % longitud or ''
    if tipo in REDEFINICION_CAMPOS:
        return '%s%s' % (REDEFINICION_CAMPOS[tipo], longitud)
    elif tipo == 'varbinary':
        return 'bytea'
    else:
        return '%s%s' % (tipo, longitud)
    
def adaptar_nombre_tabla(nombre):
    '''
    Cambia al nombre
    '''
    if NUMERO.match(nombre):
        return '"%s"' % nombre
    return nombre

def procesar_archivo(dump_sql):
    f = codecs.open(dump_sql, 'r', 'utf-8')
    sql = f.read()  
    matches = RE_SQL_CREACION_TABLA.findall(sql)



def definir_tabla(nombre_tabla, campos):
    '''
    '''
    columnas = []
    for campo in campos:
        col_sql = '%s' % adaptar_nombre_tabla(campo.group('nombre'))
        tipo = campo.group('tipo')
        col_sql += ' %s' % tipo_mssql_a_pgsql(tipo, campo.group('longitud'))
        TIPOS_UTILIZADOS.add(tipo)
        col_sql += ' %s' % campo.group('nulidad').lower()
        columnas.append(col_sql)
    campos = ',\n\t'.join(columnas)
    
    print "*"*20
    print "Campos:", campos
    print "*"*20
    
    sql = '''
        DROP TABLE IF EXISTS %(tabla)s;
        CREATE TABLE %(tabla)s (
        %(campos)s
        );
    ''' % {
           'tabla': nombre_tabla, 
           'campos': campos
           }
    sql.strip()
    return sql

curs = conn.cursor()

def es_tabla_util(nombre):
    if nombre.lower().startswith('msmerge'):
        return False
    return True

#for nombre_tabla, defincion_tabla in matches:
#    columnas =  RE_SQL_DEFINICION_COLUMNA.finditer(defincion_tabla)
#    
#    sql = definir_tabla(nombre_tabla, columnas)
#    print sql
#    curs.execute(sql)
#conn.commit()
#print len(matches)
#print TIPOS_UTILIZADOS
##print matches
##print matches; from ipdb import set_trace; set_trace()
