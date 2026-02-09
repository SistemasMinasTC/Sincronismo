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

    Chave = recordtype('Chave', 'cod_clube, cod_tipo_associado, cod_cota, cod_associado')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from TaxaFrequencia
            where
                IdTaxaFrequencia = (select PkSql from PkDePara where Tabela = 'TaxaFrequencia' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}|' || cod_tipo_associado || '|' || cod_cota as cod_cota,
            case cod_tipo_desconto
                when 'DES50' then 50.0
            end as cod_tipo_desconto,
            dat_ini_adesao,
            dat_fim_adesao,
            observacao
        from {linha_log.banco}:cota_taxa_freq as cota_taxa_freq
        where
            cod_tipo_associado = ? and
            cod_cota = ? and
            cod_associado = ?
    """,(
        chave.cod_tipo_associado,
        chave.cod_cota,
        chave.cod_associado,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update TaxaFrequencia set
            IdCota = (select PkSql from PkDePara where Tabela = 'Cota' and PkIfx = ?),
            PercentualDesconto = ?,
            DataInicioAdesao = ?,
            DataFimAdesao = ?,
            Observacao = ?,
            UltimaAlteracao = getdate()
        where
            IdTaxaFrequencia = (select PkSql from PkDePara where Tabela = 'TaxaFrequencia' and PkIfx = ?)
    """,(
            origem.cod_cota,
            origem.cod_tipo_desconto,
            origem.dat_ini_adesao,
            origem.dat_fim_adesao,
            origem.observacao,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into TaxaFrequencia
            (
                IdCota,
                PercentualDesconto,
                DataInicioAdesao,
                DataFimAdesao,
                Observacao
            ) output values (
                (select PkSql from PkDePara where Tabela = 'Cota' and PkIfx = ?) /*IdCota*/,
                ? /*PercentualDesconto*/,
                ? /*DataInicioAdesao*/,
                ? /*DataFimAdesao*/,
                ? /*Observacao*/
            )
        """,(
            origem.cod_cota,
            origem.cod_tipo_desconto,
            origem.dat_ini_adesao,
            origem.dat_fim_adesao,
            origem.observacao
        ))

        cr_sql.execute("""select ident_current('TaxaFrequencia')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('TaxaFrequencia',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'cota_taxa_freq'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
