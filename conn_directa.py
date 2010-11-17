# -*- coding: utf-8 -*-


import pymssql
from connections import virtualbox
cur = conn.cursor()
cur.execute("SELECT name FROM sysobjects WHERE type='u' ORDER BY name;")

def ejecutar_consulta(query):
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    while row:
        yield row
        row = cur.fetchone()
    cur.close()

#cant = 0
#row = cur.fetchone()
#while row:
#    from ipdb import set_trace; set_trace()
#    print "Tabla: %s" % (row[0],)
#    row = cur.fetchone()
#    cant += 1
#print "Catnidad de filas", cant
c = ejecutar_consulta("SELECT name FROM sysobjects WHERE type='u' ORDER BY name;")
for r in c:
    print "%s" % r



# if you call execute() with one argument, you can use % sign as usual
# (it loses its special meaning).
#cur.execute("SELECT * FROM persons WHERE salesrep LIKE 'J%'")

conn.close()

#You can also use iterators instead of while loop. Iterators are DB-API extensions, and are available since pymssql 1.0.