#!/usr/bin/python
#
from biblioteca.conexoes import *
from biblioteca.createSincronismoIfxSql import *
from biblioteca.createSincronismoSqlIfx import *

def nomeTabelas(sgbd='Informix', banco='minas'):
    if sgbd == 'Informix':
        bd = conecta_informix(banco)
        cr = bd.cursor()
        cr.execute('select tabname from systables where tabid >= 100 order by tabname')
    else:
        bd = conecta_mssql()
        cr = bd.cursor()
        cr.execute('select TABLE_NAME from INFORMATION_SCHEMA.TABLES order by TABLE_NAME')

    tabelas = [tab[0] for tab in cr]
    cr.close()
    return tabelas


def nomeColunas(sgbd = 'Informix', query = 'select * from dual', ordenado=False, banco='minas'):
    bd = conecta_informix(banco) if sgbd == 'Informix' else conecta_mssql()
    cr = bd.cursor()
    try:
        cr.execute(query)
    except:
        return None
    else:
        return [col[0] for col in cr.description] if not ordenado else sorted(['cod_clube',]+[col[0] for col in cr.description])

def pkTabela(sgbd, tabela, banco='minas'):
    if sgbd == 'Informix':
        bd = conecta_informix(banco)
        if not bd:
            return None

        cr = bd.cursor()

        cr.execute("""
            select
                (select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part1)||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part2),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part3),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part4),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part5),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part6),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part7),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part8),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part9),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part10),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part11),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part12),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part13),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part14),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part15),'')||
                nvl(','||(select colname from syscolumns where syscolumns.tabid = sysindexes.tabid and colno = part16),'') as pk,
                tabname,
                idxname
            from sysindexes
            inner join systables on systables.tabid = sysindexes.tabid
            where
                idxtype = 'U' and
                systables.tabname = ?
            order by idxname
        """, (tabela,))

        linha = cr.fetchone()

        return linha[0] if linha else 'rowid'
    else:
        return f'Id{tabela}'


# Teste
#
if __name__ == "__main__":
    print(pkTabela('Informix','horario_pessoa', banco='minas'))
