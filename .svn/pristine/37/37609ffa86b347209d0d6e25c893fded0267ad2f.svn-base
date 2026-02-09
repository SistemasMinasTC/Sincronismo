#!/usr/bin/env python
# -*- coding:utf-8 -*-
#

import os, sys, pyodbc

if os.name == 'nt' and hasattr(os, 'add_dll_directory'):
    informixdir = os.getenv('INFORMIXDIR', None)
    if informixdir and os.path.exists(informixdir):
        os.add_dll_directory(os.path.join(informixdir, "bin"))
else:
    os.putenv("INFORMIXDIR", "/opt/IBM/informix")
import IfxPyDbi

def conecta_informix(banco='minas'):
    if len(sys.argv) > 1:
        ambiente = sys.argv[1].upper()
    else:
        ambiente =  os.getenv('AMBIENTE_NSS')

    try:
        ifx = IfxPyDbi.connect(f"SERVER=servlxcordes;DATABASE={banco};UID=informix;PWD=BDDlnx10;DB_LOCALE=en_us.1252") if ambiente != 'PRODUCAO' else \
              IfxPyDbi.connect(f"SERVER=servlxcorpro;DATABASE={banco};UID=informix;PWD=BDDlnx10;DB_LOCALE=en_us.1252")
    except:
        ifx = None
    else:
        ifx.set_autocommit(True)
        cr = ifx.cursor()
        cr.execute("set lock mode to wait 60")
        cr.execute("set environment uselastcommitted 'NONE'")
        cr.execute("set isolation to committed read")
        cr.close()

    return ifx

def conecta_mssql():
    if len(sys.argv) > 1:
        ambiente = sys.argv[1].upper()
    else:
        ambiente =  os.getenv('AMBIENTE_NSS')

    print(ambiente if ambiente in ('PRODUCAO','HOMOLOGACAO') else 'DESENVOLVIMENTO')

    try:
        if ambiente == 'PRODUCAO':
            sql = pyodbc.connect(f"Driver={'FreeTDS' if os.name == 'posix' else 'SQL Server'};Server=servwbdsqlsgc;port=1433;uid=sa;pwd=@mil960@;database=MinasCorporativo",autocommit=True)
        elif ambiente == 'HOMOLOGACAO':
            sql = pyodbc.connect(f"Driver={'FreeTDS' if os.name == 'posix' else 'SQL Server'};Server=servwbdsqlsgc;port=1433;uid=sa;pwd=@mil960@;database=MinasCorporativoHomolog",autocommit=True)
        else:
            sql = pyodbc.connect(f"Driver={'FreeTDS' if os.name == 'posix' else 'SQL Server'};Server=servwsqldeserp;port=1433;uid=sa;pwd=@deti@20@;database=MinasCorporativo",autocommit=True)

    except:
        sql = None
    else:
        cr = sql.cursor()
        cr.execute("set transaction isolation level read uncommitted")
        cr.close()

    return sql

# Teste
#
if __name__ == "__main__":
    teste = 'teste'
    ifx = conecta_informix()
    if ifx:
        print('Conexão informix ok')
    else:
        print('Banco informix não disponível')

    sql = conecta_mssql()
    if sql:
        print('Conexão mssql ok')
    else:
        print('Banco mssql não disponível')
