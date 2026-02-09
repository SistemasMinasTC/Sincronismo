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

    Chave = recordtype('Chave', 'cod_clube, cod_tipo_associado, cod_cota, nro_fatura, dat_pagto')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from PagamentoRejeitado
            where
                IdPagamentoRejeitado = (select PkSql from PkDePara where Tabela = 'PagamentoRejeitado' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            cod_tipo_associado,
            cod_cota,
            nro_fatura,
            dat_pagto,
            vlr_pago
        from {linha_log.banco}:cota_pagto_rejeit as cota_pagto_rejeit
        where
            cod_tipo_associado = ? and
            cod_cota = ? and
            nro_fatura = ? and
            dat_pagto = to_date(?,'%d/%m/%Y')
    """,(
        chave.cod_tipo_associado,
        chave.cod_cota,
        chave.nro_fatura,
        chave.dat_pagto,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        select
            Cota.IdCota,
            Fatura.IdFatura
        from Fatura
        inner join Cota on Cota.IdCota = Fatura.IdCota
        where
            Cota.IdClube = ? and
            Cota.TipoCota = ? and
            Cota.NumeroCota = ? and
            Fatura.NumeroFatura = ?
    """,(
        origem.cod_clube,
        origem.cod_tipo_associado,
        origem.cod_cota,
        origem.nro_fatura,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    dados = Linha(*linha) if linha else None

    cr_sql.execute("""
        update PagamentoRejeitado set
            IdCota = ?,
            IdFatura = ?,
            DataPagamento = ?,
            ValorPago = ?,
            UltimaAlteracao = getdate()
        where
            IdPagamentoRejeitado = (select PkSql from PkDePara where Tabela = 'PagamentoRejeitado' and PkIfx = ?)
    """,(
            dados.IdCota,
            dados.IdFatura,
            origem.dat_pagto,
            origem.vlr_pago,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into PagamentoRejeitado
            (
                IdCota,
                IdFatura,
                DataPagamento,
                ValorPago
            ) values (
                ? /*IdCota*/,
                ? /*IdFatura*/,
                ? /*DataPagamento*/,
                ? /*ValorPago*/
            )
        """,(
            dados.IdCota,
            dados.IdFatura,
            origem.dat_pagto,
            origem.vlr_pago
        ))

        cr_sql.execute("""select ident_current('PagamentoRejeitado')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('PagamentoRejeitado',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'cota_pagto_rejeit'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
