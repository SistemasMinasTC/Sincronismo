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

    Chave = recordtype('Chave', 'cod_clube, nro_seq_agregado')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Adesao
            where
                IdAdesao = (select PkSql from PkDePara where Tabela = 'Adesao' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}|' || cod_tipo_associado ||'|'|| cod_cota as cod_cota,
            dat_inicio,
            idc_cobra_taxa = 'S' as idc_cobra_taxa,
            dat_cancel
        from {linha_log.banco}:agregado as agregado
        where
            nro_seq_agregado = ?
    """,(
        chave.nro_seq_agregado,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Adesao set
            IdCota = (select PkSql from PkDePara where Tabela = 'Cota' and PkIfx = ?),
            DataInicio = ?,
            CobraTaxa = ?,
            DataCancelamento = ?,
            UltimaAlteracao = getdate()
        where
            IdAdesao = (select PkSql from PkDePara where Tabela = 'Adesao' and PkIfx = ?)
    """,(
            origem.cod_cota,
            origem.dat_inicio,
            origem.idc_cobra_taxa,
            origem.dat_cancel,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Adesao
            (
                IdCota,
                DataInicio,
                CobraTaxa,
                DataCancelamento
            ) output inserted.IdAdesao  values (
                (select PkSql from PkDePara where Tabela = 'Cota' and PkIfx = ?) /*Cota*/,
                ? /*DataInicio*/,
                ? /*CobraTaxa*/,
                ? /*DataCancelamento*/
            )
        """,(
            origem.cod_cota,
            origem.dat_inicio,
            origem.idc_cobra_taxa,
            origem.dat_cancel
        ))

        cr_sql.execute("""select ident_current('Adesao')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Adesao',?,?)",(pkSql, linha_log.pk,))
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
            tabela,
            operacao,
            pk
        from mc_log
        where
            tabela = 'agregado'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
