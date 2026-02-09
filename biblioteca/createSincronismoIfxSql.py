#!/usr/bin/python
#
from pywebio.output import *

from biblioteca.conexoes import *
from biblioteca.createSincronismoIfxSql import *
from biblioteca.createSincronismoSqlIfx import *

def createSincronismoIfxSql(tabelaIfx: str, primaryKeyIfx: list, tabelaSql: str, primaryKeySql: list, autoIncremento: bool, dePara: dict):
    def _wherePk_(tabela):
        return f"(select PkSql from PkDePara where Tabela = '{tabela}' and PkIfx = ?)" if autoIncremento else "?"

    colunasSql = list(dePara.keys())
    colunasSqlUpdate = [dP for dP in dePara if dP not in primaryKeySql]
    colunasIfx = [col[0] for col in dePara.values()]
    colunasIfxUpdate = [dePara[dP][0] for dP in colunasSqlUpdate]

    NL='\n'

    select_cod_clube="\n            '{cod_clube}' as cod_clube," if 'cod_clube' in colunasIfx else ''

    busca_clube = '''
    if linha_log.banco == 'minas':
        cod_clube = 'MTC'
    elif linha_log.banco == 'nautico':
        cod_clube = 'MTNC'

    linha_log.pk = cod_clube+'|'+linha_log.pk
    ''' if 'cod_clube' in colunasIfx else ''

    arquivo = open(f'sincronismo/{tabelaIfx}.py','w')
    arquivo.write(f'''\
#!/usr/bin/python
#

from recordtype import recordtype

def convert(conn_ifx, conn_sql, linha_log):
    cr_sql = conn_sql.cursor()
    try:
        cr_sql.execute('create table #sincronizando (dummy char(1))')
    except:
        pass

    cr_ifx = conn_ifx.cursor()
    cr_ifx.execute('execute procedure em_sincronismo()')
    {busca_clube}
    Chave = recordtype('Chave', '{'cod_clube, ' if 'cod_clube' in colunasIfx else ''}{', '.join(primaryKeyIfx,)}')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from {tabelaSql}
            where
                {primaryKeySql[0]} = {_wherePk_(tabelaSql)}
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select {select_cod_clube}
            {f',{NL}            '.join([c if 'idc_' not in c else c+" = 'S' as "+c for c in colunasIfx if c != 'cod_clube'])}
        from {tabelaIfx}
        where
            {f' = ? and{NL}            '.join(primaryKeyIfx,)} = ?
    """,(
        chave.{f',{NL}        chave.'.join(primaryKeyIfx,)},
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update {tabelaSql} set
            {f',{NL}            '.join([col+' = ?' if not dePara[col][1] else col+" = (select PkSql from PkDePara where Tabela = '"+dePara[col][1]+"' and PkIfx = ?)," for col in colunasSqlUpdate])},
            UltimaAlteracao = getdate()
        where
            {primaryKeySql[0]} = {_wherePk_(tabelaSql)}
    """,(
            origem.{f',{NL}            origem.'.join(colunasIfxUpdate)},
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into {tabelaSql}
            (
                {f',{NL}                '.join(colunasSql,)}
            ) output inserted.{primaryKeySql[0]} values (
                {f',{NL}                '.join('? /*'+col+'*/' if not dePara[col][1] else "(select PkSql from PkDePara where Tabela = '"+dePara[col][1]+"' and PkIfx = ?) /*"+col+"*/" for col in colunasSql)}
            )
        """,(
            origem.{f',{NL}            origem.'.join(colunasIfx,)}
        ))

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('{tabelaSql}',?,?)",(pkSql, linha_log.pk,))
        cr_sql.execute("commit transaction")

    cr_sql.close()

# Teste
#
if __name__ == "__main__":
    import sys
    from biblioteca.conexoes import *

    ifx = conecta_informix()
    if not ifx:
        print('Banco informix não disponível')
        sys.exit()

    sql = conecta_mssql()
    if not sql:
        print('Banco mssql não disponível')
        sys.exit()
    cr_ifx = ifx.cursor()

    cr_ifx.execute("""
        select
            id,
            data_hora,
            banco,
            tabela,
            operacao,
            pk
        from mc_log
        where
            tabela = '{tabelaIfx}'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
''')

    arquivo.close()
