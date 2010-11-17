# coding: utf-8
'''

'''
from mssql_schema import TABLAS
from config import pg_dsn, mssql_conf
import pymssql
import psycopg2
import sys
from pgsql_schema import columna_util, columnas_utiles, NUMERO



def tiempo(f):
    from time import time
    def wrapped(*largs, **kwargs):
        t0 = time()
        retval = f(*largs, **kwargs)
        print "%s tomó %.3fs" % (f.func_name, time() - t0)
        return retval
    return wrapped
    
def select_what(todos_los_campos):
    '''
    Genera los campos del select
    '''
    columnas = columnas_utiles(todos_los_campos)
    print ', '.join([ t['tipo'] for t in columnas ])
    cols = []
    for c in columnas:
        if c['tipo'] == 'ntext':
            # http://pymssql.sourceforge.net/faq.php
            # http://msdn.microsoft.com/en-us/library/ms187928.aspx
            cols.append('CAST(%s AS VARCHAR)' % c['nombre'])
        elif c['tipo'] == 'bit':
            #cols.append('CAST(%s AS INT)' % c['nombre'])
            cols.append(("%s = CONVERT(VARCHAR(5), CASE %s "
                        "WHEN 1 THEN 'true' ELSE 'false' END)")%
                        (c['nombre'], c['nombre']))
        else:
            cols.append(c['nombre'])
    return ', '.join(cols)


def columnas_insert(cols):
    '''
    Retorna la lista de los nombres de las columnas para la isnerción en Pg
    '''
    insert_cols = []
    for c in cols:
        nombre = c['nombre']
        if NUMERO.match(nombre):
            insert_cols.append(u'"%s"' % nombre)
        else:
            insert_cols.append(nombre)
    return insert_cols

from itertools import izip

def adaptar_binarios(columnas, datos):
    tipos = [d['tipo'] for d in columnas]
    if not 'image' in tipos:
        return columnas
    
    data = []
    for dato, tipo in izip(datos, tipos):
        if tipo == 'image':
            if not dato:
                data.append('')
                continue
            baSeq = [str(psycopg2.Binary(ba))[1:-1].replace(r'"', r'\"') for ba in dato]
            # join the prep'ed byte arrays into a single string
            bas = "\"%s\"" % '","'.join(baSeq)
            # double the number of backslashes (needed because we're inserting a
            # Bytea array as opposed to a single Bytea)
            bas = bas.replace('\\', '\\\\')
            data.append(bas)
        else:
            data.append(dato)
    return data
    
@tiempo
def copia_datos(conexion_origen, conexion_destino, tablas, limpiar_destino = True, chunk_commit = 100):
    '''
    Copia datos entre conexiones
    '''
    conexion_destino.set_client_encoding('LATIN1')
    cur_orig = conexion_origen.cursor()
    cur_dest = conexion_destino.cursor()
    #from ipdb import set_trace; set_trace()
    cant_tablas, actual = len(tablas), 0
    for tabla, campos in TABLAS.iteritems():
        if tabla.lower() not in ('attachments', 'documentos'):
            continue
        print "Tabla", tabla
        cur_orig.execute('SELECT COUNT(*) FROM %s' % tabla)
        total_origen = cur_orig.fetchone()[0]
        
        sql_source = 'SELECT %s FROM %s' % (select_what(campos), tabla)
        
        print "Query: ", sql_source 
        cur_orig.execute(sql_source)
        
        
        if limpiar_destino:
            cur_dest.execute('DELETE FROM %s' % tabla)
            conexion_destino.commit()
            
        cant = 0
        col_utils = columnas_utiles(campos)
        cant_filas = len(col_utils)
        values_dest = ','.join(['%s' for i in xrange(cant_filas)])
        
        sql_dest = "INSERT INTO %s (%s) VALUES (%s);" % (tabla,
                                                        ','.join(columnas_insert(col_utils)),
                                                         values_dest)
            
        
        
        while True: 
            datos = cur_orig.fetchmany(chunk_commit)
            if not datos:
                break
            for fila in datos:
                try:
                    fila = adaptar_binarios(col_utils, fila)
                    cur_dest.execute( sql_dest, fila)
                except psycopg2.DataError, e:
                    if e.pgcode == '22P02':
                        print "Datos Invalidos"
                        print fila
                        conexion_destino.rollback()
                    else:
                        #from ipdb import set_trace; set_trace()
                        #print "Error en la fila", fila
                        raise
            cant += len(datos)
            if (cant % chunk_commit == 0):
                #print "Commit destino"
                conexion_destino.commit()
            print cant
        print cant
        conexion_destino.commit()
        actual += 1
        print "Exito en tabla %d de %d (%.2f%%)" % (actual, cant_tablas, 
                                                    float(actual) / cant_tablas * 100)
        #return

def main(argv = sys.argv):
    conexion_origen = pymssql.connect(**mssql_conf)
    conexion_destino = psycopg2.connect(pg_dsn)
    copia_datos(conexion_origen, conexion_destino, tablas = TABLAS, chunk_commit = 1)

    
        
        
if __name__ == "__main__":
    sys.exit(main())