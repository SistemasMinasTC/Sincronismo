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

    if linha_log.banco == 'minas':
        cod_clube = 'MTC'
    elif linha_log.banco == 'nautico':
        cod_clube = 'MTNC'

    Chave = recordtype('Chave', 'cod_plano, nro_parcela')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from ParcelaPlanoCobranca
            where
                IdParcelaPlanoCobranca = (select PkSql from PkDePara where Tabela = 'ParcelaPlanoCobranca' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' ||'|'|| cod_plano as cod_plano,
            nro_parcela,
            qtd_dias,
            per_parcela
        from {linha_log.banco}:parcela_plano as parcela_plano
        where
            cod_plano = ? and
            nro_parcela = ?
    """,(
        chave.cod_plano,
        chave.nro_parcela,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update ParcelaPlanoCobranca set
            IdPlanoCobranca = (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?),
            NumeroParcela = ?,
            QuantidadeDias = ?,
            Percentual = ?,
            UltimaAlteracao = getdate()
        where
            IdParcelaPlanoCobranca = (select PkSql from PkDePara where Tabela = 'ParcelaPlanoCobranca' and PkIfx = ?)
    """,(
            origem.cod_plano,
            origem.nro_parcela,
            origem.qtd_dias,
            origem.per_parcela,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into ParcelaPlanoCobranca
            (
                IdPlanoCobranca,
                NumeroParcela,
                QuantidadeDias,
                Percentual,
                UltimaAlteracao
            ) values (
                (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?) /*IdPlanoCobranca*/,
                ? /*NumeroParcela*/,
                ? /*QuantidadeDias*/,
                ? /*Percentual*/,
                getdate() /*UltimaAlteracao*/
            )
        """,(
            origem.cod_plano,
            origem.nro_parcela,
            origem.qtd_dias,
            origem.per_parcela
        ))

        cr_sql.execute("""select ident_current('ParcelaPlanoCobranca')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('ParcelaPlanoCobranca',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'parcela_plano'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
