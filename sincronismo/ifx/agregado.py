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
        cr_sql.execute("""
            delete from Adesao
            where
                IdAdesao = (select PkSql from PkDePara where Tabela = 'Adesao' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute("""
        select
            'MTC' as cod_clube, 
            agregado.cod_tipo_associado, 
            agregado.cod_cota,
            agregado.dat_inicio,
            agregado.idc_cobra_taxa = 'S' as idc_cobra_taxa,
            agregado.dat_cancel,
            nautico.cod_agregado as cod_cota_agreg
        from minas:agregado as agregado
        inner join (select distinct cod_tipo_associado,cod_cota,dat_inicio,cod_agregado from nautico:agreg_nautico) as nautico on
            nautico.cod_tipo_associado = agregado.cod_tipo_associado and
            nautico.cod_cota = agregado.cod_cota and
            nautico.dat_inicio = agregado.dat_inicio
        where
            nro_seq_agregado = ?
    """,(
        chave.nro_seq_agregado,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    if not origem:
        cr_sql.close()
        return

    cr_sql.execute("""
        update Adesao set
            IdCota =(select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?),
            DataInicio = ?,
            CobraTaxa = ?,
            DataCancelamento = ?,
            IdCotaAdesao = (select IdCota from Cota where IdClube = 'MTNC' and TipoCota = 'CC' and NumeroCota = ?),
            UltimaAlteracao = getdate()
        where
            IdAdesao = (select PkSql from PkDePara where Tabela = 'Adesao' and PkIfx = ?)
    """,(
            origem.cod_clube, 
            origem.cod_tipo_associado, 
            origem.cod_cota,
            origem.dat_inicio,
            origem.idc_cobra_taxa,
            origem.dat_cancel,
            origem.cod_cota_agreg, 
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute("""
            insert into Adesao
            (
                IdCota,
                DataInicio,
                CobraTaxa,
                DataCancelamento,
                IdCotaAdesao
            ) values (
                (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?) /*Cota*/,
                ? /*DataInicio*/,
                ? /*CobraTaxa*/,
                ? /*DataCancelamento*/,
                (select IdCota from Cota where IdClube = 'MTNC' and TipoCota = 'CC' and NumeroCota = ?) /*CotaAdesao*/
            )
        """,(
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.cod_cota,
            origem.dat_inicio,
            origem.idc_cobra_taxa,
            origem.dat_cancel, 
            origem.cod_cota_agreg, 
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
    from conexoes import *

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
            tabela = 'agregado'
        order by data_hora
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)

