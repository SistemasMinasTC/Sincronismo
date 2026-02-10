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

    if linha_log.operacao == 'del':
        excluido = json.loads(linha_log.excluido)[0]
        Linha = recordtype('Linha',excluido.keys())
        origem = Linha(**excluido)

        cr_sql.execute(f"""
            select
                case
                    when IdClube = 'MTC' then 'minas'
                    else 'nautico'
                end as IdBanco
            from Fatura
            inner join Cota on Cota.IdCota = Fatura.IdCota
            where IdFatura = ?
        """,(
            origem.IdFatura,
        ))

        linha_log.banco = cr_sql.fetchval()

        cr_ifx.execute(f"""
            delete from {linha_log.banco}:item_fat_eterno
            where
                nro_fatura = ? and
                nro_seq = ?
        """, (
            origem.NumeroFatura,
            origem.NumeroItem,
        ))

        cr_sql.close()
        return

    cr_sql.execute("""
        select
            case Cota.IdClube
                when 'MTC' then 'minas'
                else 'nautico'
            end as cod_banco,
            Fatura.NumeroFatura,
            ItemFatura.NumeroItem,
            Cota.TipoCota,
            Cota.NumeroCota,
            Associado.NPF,
            Receita.CodigoReceita,
            format(ReceitaCota.DataReceita,'yyyy-MM-dd') as DataReceita,
            format(ReceitaCota.DataReceita,'HH:mm:ss.fff') as HoraReceita,
            ReceitaCota.ValorReceita - ReceitaCota.ValorDesconto as Valor,
            isnull(ItemFatura.ValorMulta,0) + isnull(ItemFatura.ValorJuros,0) as ValorAcrescimo,
            ReceitaCota.ValorDesconto,
            PlanoCobranca.CodigoPlanoCobranca,
            ReceitaCota.NumeroDaParcela,
            Fatura.DataPagamento,
            Fatura.Status
        from ItemFatura
        inner join Fatura on Fatura.IdFatura = ItemFatura.IdFatura
        inner join Cota on Fatura.IdCota = Cota.IdCota
        inner join ReceitaCota on ReceitaCota.IdReceitaCota = ItemFatura.IdReceitaCota
        inner join Receita on Receita.IdReceita = ReceitaCota.IdReceita
        inner join PlanoCobranca on PlanoCobranca.IdPlanoCobranca = ReceitaCota.IdPlanoCobranca
        inner join Associado on Associado.IdAssociado = ReceitaCota.IdAssociado
        where
            IdItemFatura = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.close()

    linha_log.banco = origem.cod_banco    # Trata item_fatura
    #
    cr_ifx.execute(f"""
        update {linha_log.banco}:item_fatura set
            vlr_item = ?,
            vlr_desconto = ?,
            vlr_acrescimo = ?,
            cod_plano = ?,
            nro_parcela = ?,
            vlr_pago = ?
        where
            nro_fatura = ? and
            nro_seq = ?
    """,(
        origem.Valor,
        origem.ValorDesconto,
        origem.ValorAcrescimo,
        origem.CodigoPlanoCobranca,
        origem.NumeroDaParcela,
        0,
        origem.NumeroFatura,
        origem.NumeroItem
    ))

    if cr_ifx.rowcount == 0:
        cr_ifx.execute(f"""
            insert into {linha_log.banco}:item_fatura
            (
                nro_fatura,
                nro_seq,
                vlr_item,
                vlr_desconto,
                vlr_acrescimo,
                cod_plano,
                nro_parcela,
                vlr_pago
            ) values (
                ? {{nro_fatura}},
                ? {{nro_seq}},
                ? {{vlr_item}},
                ? {{vlr_desconto}},
                ? {{vlr_acrescimo}},
                ? {{cod_plano}},
                ? {{nro_parcela}},
                ? {{vlr_pago}}
            )
        """,(
            origem.NumeroFatura,
            origem.NumeroItem,
            origem.Valor,
            origem.ValorDesconto,
            origem.ValorAcrescimo,
            origem.CodigoPlanoCobranca,
            origem.NumeroDaParcela,
            0.0
        ))

    cr_ifx.execute(f"""
        select count(*) as qtde
        from {linha_log.banco}:item_fat_eterno
        where
            nro_fatura = ? and
            nro_seq = ?
    """, (
        origem.NumeroFatura,
        origem.NumeroItem,
    ))

    linha = cr_ifx.fetchone()
    existe = linha[0] > 0 if linha else False

    # Não há necessidade de update em item_fat_eterno, apenas insert
    #
    if not existe:
        try:
            cr_ifx.execute(f"""begin work""")

            cr_ifx.execute(f"""
                insert into {linha_log.banco}:item_fat_eterno
                (
                    nro_fatura,
                    nro_seq,
                    cod_tipo_associado,
                    cod_cota,
                    cod_associado,
                    cod_receita,
                    dat_receita,
                    hor_receita
                ) values (
                    ? {{nro_fatura}},
                    ? {{nro_seq}},
                    ? {{cod_tipo_associado}},
                    ? {{cod_cota}},
                    ? {{cod_associado}},
                    ? {{cod_receita}},
                    ? {{dat_receita}},
                    ? {{hor_receita}}
                )
            """,(
                origem.NumeroFatura,
                origem.NumeroItem,
                origem.TipoCota,
                origem.NumeroCota,
                origem.NPF,
                origem.CodigoReceita,
                origem.DataReceita,
                origem.HoraReceita
            ))

            # Trata item_fat_receita que é igual a item_fat_eterno para faturas não pagas
            #
            if not origem.DataPagamento and origem.Status != 'cancelada':
                cr_ifx.execute(f"""
                    insert into {linha_log.banco}:item_fat_receita
                    (
                        nro_fatura,
                        nro_seq,
                        cod_tipo_associado,
                        cod_cota,
                        cod_associado,
                        cod_receita,
                        dat_receita,
                        hor_receita
                    ) values (
                        ? {{nro_fatura}},
                        ? {{nro_seq}},
                        ? {{cod_tipo_associado}},
                        ? {{cod_cota}},
                        ? {{cod_associado}},
                        ? {{cod_receita}},
                        ? {{dat_receita}},
                        ? {{hor_receita}}
                    )
                """,(
                    origem.NumeroFatura,
                    origem.NumeroItem,
                    origem.TipoCota,
                    origem.NumeroCota,
                    origem.NPF,
                    origem.CodigoReceita,
                    origem.DataReceita,
                    origem.HoraReceita
                ))

            cr_ifx.execute(f"""commit work""")
        except Exception as erro:
            cr_ifx.execute(f"""rollback work""")
            raise Exception (erro)

    cr_ifx.close()

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

    cr_sql = sql.cursor()

    cr_sql.execute("""
        select
            id,
            data_hora,
            banco,
            tabela,
            operacao,
            pk
        from mc_log
        where
            tabela = 'ItemFatura'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)

