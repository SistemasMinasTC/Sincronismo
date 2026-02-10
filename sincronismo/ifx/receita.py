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

    linha_log.pk = cod_clube+'|'+linha_log.pk

    Chave = recordtype('Chave', 'cod_clube, cod_receita')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Receita
            where
                IdReceita = (select PkSql from PkDePara where Tabela = 'Receita' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            des_receita,
            cod_tipo_receita,
            idc_fatura = 'S' as idc_fatura,
            nro_prior_fatura,
            cod_conta_reduz,
            cod_centro_custo,
            cod_proj_contab,
            cod_receita
        from {linha_log.banco}:receita as receita
        where
            cod_receita = ?
    """,(
        chave.cod_receita,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Receita set
            IdClube = ?,
            NomeReceita = ?,
            IdTipoReceita = (select PkSql from PkDePara where Tabela = 'TipoReceita' and PkIfx = ?),
            Faturar = ?,
            PrioridadeNaFatura = ?,
            CodigoConta = ?,
            CodigoCentroCusto = ?,
            CodigoProjetoContabil = ?,
            CodigoReceita = ?,
            UltimaAlteracao = getdate()
        where
            IdReceita = (select PkSql from PkDePara where Tabela = 'Receita' and PkIfx = ?)
    """,(
            origem.cod_clube,
            origem.des_receita,
            origem.cod_tipo_receita,
            origem.idc_fatura,
            origem.nro_prior_fatura,
            origem.cod_conta_reduz,
            origem.cod_centro_custo,
            origem.cod_proj_contab,
            origem.cod_receita,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Receita
            (
                IdClube,
                NomeReceita,
                IdTipoReceita,
                Faturar,
                PrioridadeNaFatura,
                CodigoConta,
                CodigoCentroCusto,
                CodigoProjetoContabil,
                CodigoReceita
            ) values (
                ? /*IdClube*/,
                ? /*NomeReceita*/,
                (select PkSql from PkDePara where Tabela = 'TipoReceita' and PkIfx = ?) /*IdTipoReceita*/,
                ? /*Faturar*/,
                ? /*PrioridadeNaFatura*/,
                ? /*CodigoConta*/,
                ? /*CodigoCentroCusto*/,
                ? /*CodigoProjetoContabil*/,
                ? /*CodigoReceita*/
            )
        """,(
            origem.cod_clube,
            origem.des_receita,
            origem.cod_tipo_receita,
            origem.idc_fatura,
            origem.nro_prior_fatura,
            origem.cod_conta_reduz,
            origem.cod_centro_custo,
            origem.cod_proj_contab,
            origem.cod_receita
        ))

        cr_sql.execute("""select ident_current('Receita')""")

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Receita',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'receita'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
