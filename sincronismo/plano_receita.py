#!/usr/bin/python
#

from recordtype import recordtype

def convert(conn_ifx, conn_sql, linha_log):
    cr_sql = conn_sql.cursor()
    try:
        cr_sql.execute('create table #sincronizando (dummy char(1))')
    except:
        pass

    if linha_log.banco == 'minas':
        cod_clube = 'MTC'
    elif linha_log.banco == 'nautico':
        cod_clube = 'MTNC'

    linha_log.pk = cod_clube+'|'+linha_log.pk

    cr_ifx = conn_ifx.cursor()
    cr_ifx.execute('execute procedure em_sincronismo()')

    Chave = recordtype('Chave', 'cod_clube, cod_receita, cod_plano')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from ReceitaPlanoCobranca
            where
                IdReceitaPlanoCobranca = (select PkSql from PkDePara where Tabela = 'ReceitaPlanoCobranca' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}|' || cod_receita as cod_receita,
            '{cod_clube}|' || cod_plano as cod_plano
        from {linha_log.banco}:plano_receita as plano_receita
        where
            cod_receita = ? and
            cod_plano = ?
    """,(
        chave.cod_receita,
        chave.cod_plano,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update ReceitaPlanoCobranca set
            IdReceita = (select PkSql from PkDePara where Tabela = 'Receita' and PkIfx = ?),
            IdPlanoCobranca = (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?),
            UltimaAlteracao = getdate()
        where
            IdReceitaPlanoCobranca = (select PkSql from PkDePara where Tabela = 'ReceitaPlanoCobranca' and PkIfx = ?)
    """,(
            origem.cod_receita,
            origem.cod_plano,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into ReceitaPlanoCobranca
            (
                IdReceita,
                IdPlanoCobranca
            ) values (
                (select PkSql from PkDePara where Tabela = 'Receita' and PkIfx = ?) /*IdReceita*/,
                (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?) /*IdPlanoCobranca*/
            )
        """,(
            origem.cod_receita,
            origem.cod_plano
        ))

        cr_sql.execute("""select ident_current('ReceitaPlanoCobranca')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('ReceitaPlanoCobranca',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'plano_receita'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
