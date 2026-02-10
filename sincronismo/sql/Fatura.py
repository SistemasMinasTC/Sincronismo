#!/usr/bin/python
#

import json
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
            from Cota
            where IdCota = ?
        """,(
            origem.IdCota,
        ))

        linha_log.banco = cr_sql.fetchval()

        cr_ifx.execute(f"""
            delete from {linha_log.banco}:fatura
            where
                nro_fatura = ?
        """, (
            origem.NumeroFatura,
        ))

        cr_sql.close()
        return

    cr_sql.execute("""
        select
            NumeroFatura,
            DataVencimento,
            case EmissaoMensal
                when 1 then 'U'
                else 'L'
            end as EmissaoMensal,
            TipoCota,
            NumeroCota,
            DataEmissao,
            case Status
                when 'baixa banco' then 'B'
                when 'baixa manual' then 'M'
                when 'cancelada' then 'C'
                when 'tesouraria' then 'T'
                when 'aberta' then 'E'
                when 'débito automático' then 'D'
                else '?'
            end as Status,
            Valor,
            ValorMulta,
            ValorJuros,
            0.0 as ValorAcrescimo,
            ValorDesconto,
            DataPagamento,
            ValorPago,
            isnull(Matricula,0) as Matricula,
            DataEnvioEmail,
            DataEnvioSMS1,
            DataEnvioSMS2,
            CodigoBarra,
            LinhaDigitavel,
            DataRegistro,
            DataRegistroCancelamento,
            PIX,
            IdentificacaoMensagemMailgun,
            IdClube
        from Fatura
        inner join Cota on Cota.IdCota = Fatura.IdCota
        left join Usuario on Usuario.IdUsuario = Fatura.IdUsuario
        where
            IdFatura = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    origem = Linha(*linha) if (linha := cr_sql.fetchone()) else None

    cr_sql.close()

    Chave = recordtype('Chave', 'nro_fatura')
    chave = Chave(origem.NumeroFatura)

    linha_log.banco = 'minas' if origem.IdClube == 'MTC' else 'nautico'

    if linha_log.operacao == 'com':
        cr_ifx.execute(f'execute procedure status_cota ({origem.TipoCota},{origem.NumeroCota})')
        return

    cr_ifx.execute(f"""
        update {linha_log.banco}:fatura set
            dat_vencto = ?,
            idt_fatura = ?,
            cod_tipo_associado = ?,
            cod_cota = ?,
            dat_fatura = ?,
            idt_status = ?,
            vlr_fatura = ?,
            vlr_multa = ?,
            vlr_juros = ?,
            vlr_acrescimo = ?,
            vlr_desconto = ?,
            dat_pagto = ?,
            vlr_pago = ?,
            mla_funcionario = ?,
            dat_envio_email = ?,
            dat_envio_sms_1 = ?,
            dat_envio_sms_2 = ?,
            cod_barra = ?,
            lin_digitavel = ?,
            dat_registro_fatura = ?,
            dat_cancelamento_registro = ?,
            cod_pix = ?,
            idf_mensagem_mailgun = ?
        where
            nro_fatura = ?
    """,(
            origem.DataVencimento,
            origem.EmissaoMensal,
            origem.TipoCota,
            origem.NumeroCota,
            origem.DataEmissao,
            origem.Status,
            origem.Valor,
            origem.ValorMulta,
            origem.ValorJuros,
            origem.ValorAcrescimo,
            origem.ValorDesconto,
            origem.DataPagamento,
            origem.ValorPago,
            origem.Matricula,
            origem.DataEnvioEmail,
            origem.DataEnvioSMS1,
            origem.DataEnvioSMS2,
            origem.CodigoBarra,
            origem.LinhaDigitavel,
            origem.DataRegistro,
            origem.DataRegistroCancelamento,
            origem.PIX,
            origem.IdentificacaoMensagemMailgun,
            chave.nro_fatura,
    ))

    if cr_ifx.rowcount == 0:
        cr_ifx.execute(f"""
            insert into {linha_log.banco}:fatura
            (
                nro_fatura,
                dat_vencto,
                idt_fatura,
                cod_tipo_associado,
                cod_cota,
                dat_fatura,
                idt_status,
                vlr_fatura,
                vlr_multa,
                vlr_juros,
                vlr_acrescimo,
                vlr_desconto,
                dat_pagto,
                vlr_pago,
                mla_funcionario,
                dat_envio_email,
                dat_envio_sms_1,
                dat_envio_sms_2,
                cod_barra,
                lin_digitavel,
                dat_registro_fatura,
                dat_cancelamento_registro,
                cod_pix,
                idf_mensagem_mailgun
            ) values (
                ? {{nro_fatura}},
                ? {{dat_vencto}},
                ? {{idt_fatura}},
                ? {{cod_tipo_associado}},
                ? {{cod_cota}},
                ? {{dat_fatura}},
                ? {{idt_status}},
                ? {{vlr_fatura}},
                ? {{vlr_multa}},
                ? {{vlr_juros}},
                ? {{vlr_acrescimo}},
                ? {{vlr_desconto}},
                ? {{dat_pagto}},
                ? {{vlr_pago}},
                ? {{mla_funcionario}},
                ? {{dat_envio_email}},
                ? {{dat_envio_sms_1}},
                ? {{dat_envio_sms_2}},
                ? {{cod_barra}},
                ? {{lin_digitavel}},
                ? {{dat_registro_fatura}},
                ? {{dat_cancelamento_registro}},
                ? {{cod_pix}},
                ? {{idf_mensagem_mailgun}}
            )
        """,(
            origem.NumeroFatura,
            origem.DataVencimento,
            origem.EmissaoMensal,
            origem.TipoCota,
            origem.NumeroCota,
            origem.DataEmissao,
            origem.Status,
            origem.Valor,
            origem.ValorMulta,
            origem.ValorJuros,
            origem.ValorAcrescimo,
            origem.ValorDesconto,
            origem.DataPagamento,
            origem.ValorPago,
            origem.Matricula,
            origem.DataEnvioEmail,
            origem.DataEnvioSMS1,
            origem.DataEnvioSMS2,
            origem.CodigoBarra,
            origem.LinhaDigitavel,
            origem.DataRegistro,
            origem.DataRegistroCancelamento,
            origem.PIX,
            origem.IdentificacaoMensagemMailgun
        ))

    # Remove de item_fat_receita se a fatura foi cancelada
    #

    if origem.Status == 'C':
        cr_ifx.execute(f"""
            delete from {linha_log.banco}:item_fat_receita
            where
                nro_fatura = ?
        """,(
            origem.NumeroFatura,
        ))

    # Tratamento do código de restricao para portaria
    #
    cr_ifx.execute(f"""
        update {linha_log.banco}:pessoa_fisica
        set
            cod_tipo_restricao = restricao(cod_pessoa,today)
        where
            idt_pessoa = 1 and
            cod_pessoa in
            (
               select cod_associado
               from {linha_log.banco}:cota_associado
               where
                  cod_tipo_associado = ? and
                  cod_cota = ?
            )
    """,(
        origem.TipoCota,
        origem.NumeroCota
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
            pk,
            excluido
        from mc_log
        where
            tabela = 'Fatura'
            and operacao = 'com'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)

