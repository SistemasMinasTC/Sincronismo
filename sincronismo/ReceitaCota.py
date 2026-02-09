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

    cr_sql.execute(f"""
        select
            ReceitaCota.IdReceitaCota,
            case
                when Cota.IdClube = 'MTC' then 'minas'
                when Cota.IdClube = 'MTNC' then 'nautico'
            end as CodigoClube,
            Cota.TipoCota,
            Cota.NumeroCota,
            Associado.NPF,
            Receita.CodigoReceita,
            format(ReceitaCota.DataReceita,'yyyy-MM-dd') as DataReceita,
            format(ReceitaCota.DataReceita,'HH:mm:ss.fff') as HoraReceita,
            (select substring(PkIfx,charindex('|', PkIfx)+1,len(PkIfx)) from PkDePara where Tabela = 'PlanoCobranca' and PkSql = IdPlanoCobranca) as CodigoPlanoCobranca,
            ReceitaCota.NumeroDaParcela,
            ReceitaCota.DataVencimento,
            isnull(ReceitaCota.ValorReceita,0.0) as ValorReceita,
            isnull(ReceitaCota.ValorDesconto,0.0) as ValorDesconto,
            ReceitaCota.DataPagto,
            isnull(ReceitaCota.ValorPago,0) as ValorPago,
            isnull(ItemFatura.ValorMulta,0) as ValorMulta,
            isnull(ItemFatura.ValorJuros,0) as ValorJuros,
            case
                when ReceitaCota.Status = 'Cancelada' then 'C'
                when ReceitaCota.Status = 'Aberta' then 'E'
                when ReceitaCota.Status = 'Excluida' then 'X'
                when ItemFatura.Status = 'baixa banco' then 'B'
                when ItemFatura.Status = 'baixa manual' then 'M'
                when ItemFatura.Status = 'débito automático' then 'D'
                else 'T'
            end as Status,
            case
                when ReceitaCota.Status = 'Cancelada' then isnull(DataCancelamento,DataReceita)
                else isnull(ReceitaCota.DataInformacaoPagamento,ReceitaCota.DataPagto)
            end as DataInformacaoPagamento,
            (select substring(PkIfx,charindex('|', PkIfx)+1,len(PkIfx)) from PkDePara where Tabela = 'Acerto' and PkSql = IdAcerto) as NumeroAcerto,
            (select PkIfx from PkDePara where Tabela = 'Motivo' and PkSql = ReceitaCota.IdMotivoCancelamento) as CodigoMotivoCancelamento,
            isnull((select Matricula from Usuario where Usuario.IdUsuario = ReceitaCota.IdUsuario),0) as Matricula,
            NumeroFatura
        from ReceitaCota
        inner join Associado on Associado.IdAssociado = ReceitaCota.IdAssociado
        inner join Cota on Cota.IdCota = Associado.IdCota
        inner join Receita on Receita.IdReceita = ReceitaCota.IdReceita
        left join
        (
            select Fatura.Status,Fatura.NumeroFatura,ItemFatura.*
            from Fatura
            inner join ItemFatura on ItemFatura.IdFatura = Fatura.IdFatura
            where
                DataPagamento is not null
        ) as ItemFatura on ItemFatura.IdReceitaCota = ReceitaCota.IdReceitaCota
        where
            ReceitaCota.IdReceitaCota = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    origem = Linha(*linha) if (linha := cr_sql.fetchone()) else None

    cr_sql.close()

    cr_ifx.execute(f"""
        select trim(nom_view) as nom_view
        from {origem.CodigoClube}:_movimentacao_receita_ as _movimentacao_receita_
        where
            _movimentacao_receita_.cod_tipo_associado = ? and
            _movimentacao_receita_.cod_cota = ? and
            _movimentacao_receita_.cod_associado = ? and
            _movimentacao_receita_.cod_receita = ? and
            _movimentacao_receita_.dat_receita = ?::datetime year to day and
            _movimentacao_receita_.hor_receita = ?
    """,(
        origem.TipoCota,
        origem.NumeroCota,
        origem.NPF,
        origem.CodigoReceita,
        origem.DataReceita,
        origem.HoraReceita,
    ))

    tabela_ifx = linha[0] if (linha := cr_ifx.fetchone()) else None

    if linha_log.operacao == 'del':
        if tabela_ifx:
            cr_ifx.execute(f"""
                delete from {origem.CodigoClube}:{tabela_ifx}
                where
                    cod_tipo_associado = ? and
                    cod_cota = ? and
                    cod_associado = ? and
                    cod_receita = ? and
                    dat_receita = ? and
                    hor_receita = ?
            """, (
                origem.TipoCota,
                origem.NumeroCota,
                origem.NPF,
                origem.CodigoReceita,
                origem.DataReceita,
                origem.HoraReceita
        ))

        return

    cr_ifx.execute(f"""
        select count(*) as quantidade
        from {origem.CodigoClube}:cota_associado
        where
            cod_tipo_associado = ? and
            cod_cota = ? and
            cod_associado = ? and
            to_char(dat_incl_associado,'%Y-%m-%d') <= ?
    """, (
        origem.TipoCota,
        origem.NumeroCota,
        origem.NPF,
        origem.DataReceita
    ))

    existeAssociado = True if cr_ifx.fetchone()[0] > 0 else False

    if origem.DataPagto or origem.Status == 'C': # posicao_cota
        tabela = 'posicao_cota' if existeAssociado and origem.Status != 'X' else 'posicao_assoc_excl'

        if tabela != tabela_ifx and tabela_ifx:
            if tabela_ifx == 'receita_cota':
                cr_ifx.execute(f"""
                    delete from {origem.CodigoClube}:item_fat_receita
                    where
                        cod_tipo_associado = ? and
                        cod_cota = ? and
                        cod_associado = ? and
                        cod_receita = ? and
                        dat_receita = ? and
                        hor_receita = ?
                """, (
                    origem.TipoCota,
                    origem.NumeroCota,
                    origem.NPF,
                    origem.CodigoReceita,
                    origem.DataReceita,
                    origem.HoraReceita
                ))

            cr_ifx.execute(f"""
                delete from {origem.CodigoClube}:{tabela_ifx}
                where
                    cod_tipo_associado = ? and
                    cod_cota = ? and
                    cod_associado = ? and
                    cod_receita = ? and
                    dat_receita = ? and
                    hor_receita = ?
            """, (
                origem.TipoCota,
                origem.NumeroCota,
                origem.NPF,
                origem.CodigoReceita,
                origem.DataReceita,
                origem.HoraReceita
            ))

        cr_ifx.execute(f"""
            update {origem.CodigoClube}:{tabela} set
                cod_plano = ?,
                nro_parcela = ?,
                dat_vencimento = ?,
                vlr_receita = ?,
                vlr_desconto = ?,
                dat_pagto = ?,
                vlr_pago = ?,
                vlr_multa = ?,
                vlr_juros = ?,
                vlr_acrescimo = ?,
                idt_status = ?,
                mla_funcionario = ?,
                dat_gravacao = ?,
                nro_fatura = ?
            where
                cod_tipo_associado = ? and
                cod_cota = ? and
                cod_associado = ? and
                cod_receita = ? and
                dat_receita = ? and
                hor_receita = ?
        """,(
                origem.CodigoPlanoCobranca,
                origem.NumeroDaParcela,
                origem.DataVencimento,
                origem.ValorReceita,
                origem.ValorDesconto,
                origem.DataPagto,
                origem.ValorPago,
                origem.ValorMulta,
                origem.ValorJuros,
                origem.ValorMulta + origem.ValorJuros,
                origem.Status,
                origem.Matricula,
                origem.DataInformacaoPagamento,
                origem.NumeroFatura,

                origem.TipoCota,
                origem.NumeroCota,
                origem.NPF,
                origem.CodigoReceita,
                origem.DataReceita,
                origem.HoraReceita,
        ))

        if cr_ifx.rowcount == 0:
            cr_ifx.execute(f"""
                insert into {origem.CodigoClube}:{tabela}
                (
                    cod_tipo_associado,
                    cod_cota,
                    cod_associado,
                    cod_receita,
                    dat_receita,
                    hor_receita,
                    cod_plano,
                    nro_parcela,
                    dat_vencimento,
                    vlr_receita,
                    vlr_desconto,
                    dat_pagto,
                    vlr_pago,
                    vlr_multa,
                    vlr_juros,
                    vlr_acrescimo,
                    idt_status,
                    mla_funcionario,
                    dat_gravacao,
                    nro_fatura,
                    vlr_abatimento
                ) values (
                    ? {{cod_tipo_associado}},
                    ? {{cod_cota}},
                    ? {{cod_associado}},
                    ? {{cod_receita}},
                    ? {{dat_receita}},
                    ? {{hor_receita}},
                    ? {{cod_plano}},
                    ? {{nro_parcela}},
                    ? {{dat_vencimento}},
                    ? {{vlr_receita}},
                    ? {{vlr_desconto}},
                    ? {{dat_pagto}},
                    ? {{vlr_pago}},
                    ? {{vlr_multa}},
                    ? {{vlr_juros}},
                    ? {{vlr_acrescimo}},
                    ? {{idt_status}},
                    ? {{mla_funcionario}},
                    ? {{dat_gravacao}},
                    ? {{nro_fatura}},
                    0 {{vlr_abatimento}}
                )
            """,(
                origem.TipoCota,
                origem.NumeroCota,
                origem.NPF,
                origem.CodigoReceita,
                origem.DataReceita,
                origem.HoraReceita,
                origem.CodigoPlanoCobranca,
                origem.NumeroDaParcela,
                origem.DataVencimento,
                origem.ValorReceita,
                origem.ValorDesconto,
                origem.DataPagto,
                origem.ValorPago,
                origem.ValorMulta,
                origem.ValorJuros,
                origem.ValorMulta + origem.ValorJuros,
                origem.Status,
                origem.Matricula,
                origem.DataInformacaoPagamento,
                origem.NumeroFatura
            ))
    else: # receita_cota
        tabela = 'receita_cota' if existeAssociado and origem.Status != 'X' else 'receita_assoc_excl'

        if tabela != tabela_ifx and tabela_ifx:
            cr_ifx.execute(f"""
                delete from {origem.CodigoClube}:{tabela_ifx}
                where
                    cod_tipo_associado = ? and
                    cod_cota = ? and
                    cod_associado = ? and
                    cod_receita = ? and
                    dat_receita = ? and
                    hor_receita = ?
            """, (
                origem.TipoCota,
                origem.NumeroCota,
                origem.NPF,
                origem.CodigoReceita,
                origem.DataReceita,
                origem.HoraReceita
            ))

        cr_ifx.execute(f"""
            update {origem.CodigoClube}:{tabela} set
                cod_plano = ?,
                nro_parcela = ?,
                dat_vencimento = ?,
                vlr_receita = ?,
                vlr_desconto = ?,
                mla_funcionario = ?
            where
                cod_tipo_associado = ? and
                cod_cota = ? and
                cod_associado = ? and
                cod_receita = ? and
                dat_receita = ? and
                hor_receita = ?
        """,(
                origem.CodigoPlanoCobranca,
                origem.NumeroDaParcela,
                origem.DataVencimento,
                origem.ValorReceita,
                origem.ValorDesconto,
                origem.Matricula,

                origem.TipoCota,
                origem.NumeroCota,
                origem.NPF,
                origem.CodigoReceita,
                origem.DataReceita,
                origem.HoraReceita,
        ))

        if cr_ifx.rowcount == 0:
            cr_ifx.execute(f"""
                insert into {origem.CodigoClube}:{tabela}
                (
                    cod_tipo_associado,
                    cod_cota,
                    cod_associado,
                    cod_receita,
                    dat_receita,
                    hor_receita,
                    cod_plano,
                    nro_parcela,
                    dat_vencimento,
                    vlr_receita,
                    vlr_desconto,
                    mla_funcionario,
                    per_juros,
                    per_multa
                ) values (
                    ? {{cod_tipo_associado}},
                    ? {{cod_cota}},
                    ? {{cod_associado}},
                    ? {{cod_receita}},
                    ? {{dat_receita}},
                    ? {{hor_receita}},
                    ? {{cod_plano}},
                    ? {{nro_parcela}},
                    ? {{dat_vencimento}},
                    ? {{vlr_receita}},
                    ? {{vlr_desconto}},
                    ? {{mla_funcionario}},
                    1 {{per_juros}},
                    2 {{per_multa}}
                )
            """,(
                origem.TipoCota,
                origem.NumeroCota,
                origem.NPF,
                origem.CodigoReceita,
                origem.DataReceita,
                origem.HoraReceita,
                origem.CodigoPlanoCobranca,
                origem.NumeroDaParcela,
                origem.DataVencimento,
                origem.ValorReceita,
                origem.ValorDesconto,
                origem.Matricula
            ))

    cr_ifx.execute(f"""
        select idf_movimentacao
        from {origem.CodigoClube}:_movimentacao_receita_ as _movimentacao_receita_
        where
            cod_tipo_associado = ? and
            cod_cota = ? and
            cod_associado = ? and
            cod_receita = ? and
            dat_receita = ? and
            hor_receita = ?
    """,(
            origem.TipoCota,
            origem.NumeroCota,
            origem.NPF,
            origem.CodigoReceita,
            origem.DataReceita,
            origem.HoraReceita,
    ))

    idf_movimentacao =  linha[0] if (linha := cr_ifx.fetchone()) else None

    if idf_movimentacao:
        cr_sql = conn_sql.cursor()

        cr_sql.execute("""
            update ReceitaCota
            set
                idf_movimentacao = ?
            where
                IdReceitaCota = ?
        """,(
            idf_movimentacao,
            linha_log.pk
        ))

        cr_sql.close()



    # trata item_cancel_rec
    #
    if origem.NumeroAcerto:
        cr_ifx.execute(f"""
            select count(*) as qtd
            from {origem.CodigoClube}:acerto_cancel_rec
            where
                nro_acerto_cancel = ? and
                cod_tipo_associado = ? and
                cod_cota = ? and
                cod_associado = ? and
                cod_receita = ? and
                dat_receita = ? and
                hor_receita = ?
        """,(
            origem.NumeroAcerto,
            origem.TipoCota,
            origem.NumeroCota,
            origem.NPF,
            origem.CodigoReceita,
            origem.DataReceita,
            origem.HoraReceita
        ))

        existe = linha[0] if (linha := cr_ifx.fetchone()) else False

        if not existe and origem.CodigoReceita not in (150,151):
            cr_ifx.execute(f"""
                insert into {origem.CodigoClube}:acerto_cancel_rec
                (
                   nro_acerto_cancel,
                   cod_tipo_associado,
                   cod_cota,
                   cod_associado,
                   cod_receita,
                   dat_receita,
                   hor_receita
                ) values (
                   ? {{nro_acerto_cancel}},
                   ? {{cod_tipo_associado}},
                   ? {{cod_cota}},
                   ? {{cod_associado}},
                   ? {{cod_receita}},
                   ? {{dat_receita}},
                   ? {{hor_receita}}
                )
            """,(
                origem.NumeroAcerto,
                origem.TipoCota,
                origem.NumeroCota,
                origem.NPF,
                origem.CodigoReceita,
                origem.DataReceita,
                origem.HoraReceita
            ))
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
            tabela = 'ReceitaCota'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)

