'''
Created on 27/07/2010

@author: defo
'''

import sys
from config import mssql_conf, pg_dsn
import psycopg2
import pymssql

def main(argv = sys.argv):
    conexion_origen = pymssql.connect(**mssql_conf)
    conexion_destino = psycopg2.connect(pg_dsn)
    cur_orig, cur_dest = conexion_origen.cursor(), conexion_destino.cursor()
    
    for nombre in ('attachments', 'documentos'):
        print nombre
        cur_orig.execute('select count(*) from %s' % nombre)
        print cur_orig.fetchall()
        cur_dest.execute('select count(*) from %s' % nombre)
        print cur_dest.fetchall()

if __name__ == '__main__':
    sys.exit(main())