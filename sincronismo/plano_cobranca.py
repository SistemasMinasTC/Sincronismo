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

    linha_log.pk = cod_clube + '|' + linha_log.pk
    Chave = recordtype('Chave', 'cod_clube, cod_plano')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from PlanoCobranca
            where
                IdPlanoCobranca = (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            des_plano,
            idc_corr_monet = 'S' as idc_corr_monet,
            per_multa,
            per_juros,
            per_acrescimo,
            trim
            (
                case idt_incide_periodo
                    when 'D' then 'diario'
                    when 'M' then 'mensal'
                end
            ) as idt_incide_periodo
        from {linha_log.banco}:plano_cobranca as plano_cobranca
        where
            cod_plano = ?
    """,(
        chave.cod_plano,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update PlanoCobranca set
            IdClube = ?,
            CodigoPlanoCobranca = ?,
            NomePlano = ?,
            TemCorrecaoMonetario = ?,
            PercentualMulta = ?,
            PercentaulJuros = ?,
            PercentualAcrescimo = ?,
            PeriodoCorrecao = ?,
            UltimaAlteracao = getdate()
        where
            IdPlanoCobranca = (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?)
    """,(
            origem.cod_clube,
            chave.cod_plano,
            origem.des_plano,
            origem.idc_corr_monet,
            origem.per_multa,
            origem.per_juros,
            origem.per_acrescimo,
            origem.idt_incide_periodo,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into PlanoCobranca
            (
                IdClube,
                CodigoPlanoCobranca,
                NomePlano,
                TemCorrecaoMonetario,
                PercentualMulta,
                PercentaulJuros,
                PercentualAcrescimo,
                PeriodoCorrecao
            ) output values (
                ? /*IdClube*/,
                ? /*NomePlano*/,
                ? /*TemCorrecaoMonetario*/,
                ? /*PercentualMulta*/,
                ? /*PercentaulJuros*/,
                ? /*PercentualAcrescimo*/,
                ? /*PeriodoCorrecao*/
            )
        """,(
            origem.cod_clube,
            chave.cod_plano,
            origem.des_plano,
            origem.idc_corr_monet,
            origem.per_multa,
            origem.per_juros,
            origem.per_acrescimo,
            origem.idt_incide_periodo
        ))

        cr_sql.execute("""select ident_current('PlanoCobranca')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('PlanoCobranca',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'plano_cobranca'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
