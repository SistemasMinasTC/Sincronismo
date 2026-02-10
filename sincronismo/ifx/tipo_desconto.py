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

    Chave = recordtype('Chave', 'cod_tipo_desconto')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from ClasseCota
            where
                IdClasseCota = (select PkSql from PkDePara where Tabela = 'ClasseCota' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            cod_tipo_associado,
            des_tipo_desconto,
            per_desconto,
            cod_tipo_desconto
        from {linha_log.banco}:tipo_desconto as tipo_desconto
        cross join (select 'QT' as cod_tipo_associado from dual union select 'CT' from dual where dbinfo('DBNAME') = 'minas')
        where
            cod_tipo_desconto = ?
    """,(
        chave.cod_tipo_desconto,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    linha_log_original = linha_log.pk

    for linha in cr_ifx:

        origem = Linha(*linha) if linha else None
        linha_log.pk = cod_clube+'|'+origem.cod_tipo_associado+'|'+linha_log_original

        cr_sql.execute("""
            update ClasseCota set
                IdClube = ?,
                TipoCota = ?,
                Nome = ?,
                PercentualDescontoCondominio = ?,
                CodigoClasse = ?,
                UltimaAlteracao = getdate()
            where
                IdClasseCota = (select PkSql from PkDePara where Tabela = 'ClasseCota' and PkIfx = ?)
        """,(
                origem.cod_clube,
                origem.cod_tipo_associado,
                origem.des_tipo_desconto,
                origem.per_desconto,
                origem.cod_tipo_desconto,
                linha_log.pk,
        ))

        if cr_sql.rowcount == 0:
            cr_sql.execute('begin transaction')

            cr_sql.execute(f"""
                insert into ClasseCota
                (
                    IdClube,
                    TipoCota,
                    Nome,
                    PercentualDescontoCondominio,
                    CodigoClasse
                ) output inserted.IdClasseCota values (
                    ? /*IdClube*/,
                    ? /*TipoCota*/,
                    ? /*Nome*/,
                    ? /*PercentualDescontoCondominio*/,
                    ? /*CodigoClasse*/
                )
            """,(
                origem.cod_clube,
                origem.cod_tipo_associado,
                origem.des_tipo_desconto,
                origem.per_desconto,
                origem.cod_tipo_desconto,
            ))

            pkSql = cr_sql.fetchval()

            cr_sql.execute("insert into PkDePara values ('ClasseCota',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'tipo_desconto'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
