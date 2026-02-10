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

    Chave = recordtype('Chave', 'cod_clube, nro_fatura')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Fatura
            where
                IdClube = ? and
                NumeroFatura = ?
        """, (
            chave.cod_clube,
            chave.nro_fatura,
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            fatura.nro_fatura,
            fatura.idt_fatura = 'U' as idt_fatura,
            fatura.dat_fatura,
            fatura.dat_vencto,
            trim
            (
                case fatura.idt_status
                    when 'C' then 'cancelada'
                    when 'E' then 'aberta'
                    when 'B' then 'baixa banco'
                    when 'D' then 'débito automático'
                    when 'G' then 'G???'
                    when 'M' then 'baixa manual'
                    when 'T' then 'tesouraria'
                    when 'l' then 'l???'
                end
            ) as idt_status,
            '{cod_clube}' as cod_clube,
            fatura.cod_tipo_associado,
            fatura.cod_cota,
            fatura.vlr_fatura,
            fatura.vlr_multa,
            fatura.vlr_juros,
            fatura.vlr_desconto,
            case when fatura.idt_status <> 'C' then dat_pagto end as dat_pagto,
            fatura.vlr_pago,
            fatura.dat_registro_fatura,
            fatura.dat_envio_email,
            fatura.dat_envio_sms_1,
            fatura.dat_envio_sms_2,
            fatura.cod_barra,
            fatura.lin_digitavel,
            fatura.cod_pix,
            fatura.idf_mensagem_mailgun,
            fatura.mla_funcionario,
            (select response::json::lvarchar(4096) from {linha_log.banco}:log_santander as log_santander where idf_log = (select max(idf_log) from {linha_log.banco}:log_santander as ultimo where ultimo.nom_operacao = 'registro' and ultimo.nro_fatura = fatura.nro_fatura)) as bankslip,
            (select response::json::lvarchar(4096) from {linha_log.banco}:log_santander as log_santander where idf_log = (select max(idf_log) from {linha_log.banco}:log_santander as ultimo where ultimo.nom_operacao <> 'registro' and ultimo.nro_fatura = fatura.nro_fatura)) as settlement,
            (select webhook::json::lvarchar(4096) from {linha_log.banco}:log_webhook as log_webhook where idf_webhook = (select max(idf_webhook) from {linha_log.banco}:log_webhook as ultimo where ultimo.nro_fatura = fatura.nro_fatura)) as webhook,
            fatura_cancelada.dat_cancelamento as dat_cancelamento,
            fatura_cancelada.cod_motivo,
            fatura_cancelada.des_observacao,
            fatura_cancelada.mla_funcionario as mla_funcionario_cancelamento
        from {linha_log.banco}:fatura as fatura
        left join {linha_log.banco}:fatura_cancelada as fatura_cancelada on fatura_cancelada.nro_fatura = fatura.nro_fatura
        where
            fatura.nro_fatura = ?
    """,(
        chave.nro_fatura,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Fatura set
            EmissaoMensal = ?,
            DataEmissao = ?,
            DataVencimento = ?,
            Status = ?,
            Valor = ?,
            ValorMulta = ?,
            ValorJuros = ?,
            ValorDesconto = ?,
            DataPagamento = ?,
            ValorPago = ?,
            DataRegistro = ?,
            DataEnvioEmail = ?,
            DataEnvioSMS1 = ?,
            DataEnvioSMS2 = ?,
            CodigoBarra = ?,
            LinhaDigitavel = ?,
            PIX = ?,
            IdentificacaoMensagemMailgun = ?,
            BankslipSantander = ?,
            SettlementSantander = ?,
            WebhookSantander = ?,
            IdUsuario = (select max(IdUsuario) from Usuario where Matricula = ?),
            DataCancelamento = ?,
            IdMotivoCancelamento = (select PkSql from PkDePara where Tabela = 'Motivo' and PkIfx = ?),
            Observacao = ?,
            IdUsuarioCancelamento = (select max(IdUsuario) from Usuario where Matricula = ?),
            UltimaAlteracao = getdate()
        where
            IdCota = (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?) and
            NumeroFatura = ?
    """,(
            origem.idt_fatura,
            origem.dat_fatura,
            origem.dat_vencto,
            origem.idt_status,
            origem.vlr_fatura,
            origem.vlr_multa,
            origem.vlr_juros,
            origem.vlr_desconto,
            origem.dat_pagto,
            origem.vlr_pago,
            origem.dat_registro_fatura,
            origem.dat_envio_email,
            origem.dat_envio_sms_1,
            origem.dat_envio_sms_2,
            origem.cod_barra,
            origem.lin_digitavel,
            origem.cod_pix,
            origem.idf_mensagem_mailgun,
            origem.bankslip,
            origem.settlement,
            origem.webhook,
            origem.mla_funcionario,
            origem.dat_cancelamento,
            origem.cod_motivo,
            origem.des_observacao,
            origem.mla_funcionario_cancelamento,

            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.cod_cota,
            chave.nro_fatura,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute(f"""
            insert into Fatura
            (
                NumeroFatura,
                EmissaoMensal,
                IdCota,
                DataEmissao,
                DataVencimento,
                Status,
                Valor,
                ValorMulta,
                ValorJuros,
                ValorDesconto,
                DataPagamento,
                ValorPago,
                DataRegistro,
                DataEnvioEmail,
                DataEnvioSMS1,
                DataEnvioSMS2,
                CodigoBarra,
                LinhaDigitavel,
                PIX,
                IdentificacaoMensagemMailgun,
                BankslipSantander,
                SettlementSantander,
                WebhookSantander,
                IdUsuario,
                DataCancelamento,
                IdMotivoCancelamento,
                Observacao,
                IdUsuarioCancelamento
            ) values (
                ? /*NumeroFatura*/,
                ? /*EmissaoMensal*/,
                (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?) /*IdCota*/,
                ? /*DataEmissao*/,
                ? /*DataVencimento*/,
                ? /*Status*/,
                ? /*Valor*/,
                ? /*ValorMulta*/,
                ? /*ValorJuros*/,
                ? /*ValorDesconto*/,
                ? /*DataPagamento*/,
                ? /*ValorPago*/,
                ? /*DataRegistro*/,
                ? /*DataEnvioEmail*/,
                ? /*DataEnvioSMS1*/,
                ? /*DataEnvioSMS2*/,
                ? /*CodigoBarra*/,
                ? /*LinhaDigitavel*/,
                ? /*PIX*/,
                ? /*IdentificacaoMensagemMailgun*/,
                ? /*BankslipSantander*/,
                ? /*SettlementSantander*/,
                ? /*WebhookSantander*/,
                (select max(IdUsuario) from Usuario where Matricula = ?) /*IdUsuario*/,
                ? /*DataCancelamento*/,
                (select PkSql from PkDePara where Tabela = 'Motivo' and PkIfx = ?) /*IdMotivoCancelamento*/,
                ? /*Observacao*/,
                (select max(IdUsuario) from Usuario where Matricula = ?) /*IdUsuarioCancelamento*/
            )
        """,(
            origem.nro_fatura,
            origem.idt_fatura,
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.cod_cota,
            origem.dat_fatura,
            origem.dat_vencto,
            origem.idt_status,
            origem.vlr_fatura,
            origem.vlr_multa,
            origem.vlr_juros,
            origem.vlr_desconto,
            origem.dat_pagto,
            origem.vlr_pago,
            origem.dat_registro_fatura,
            origem.dat_envio_email,
            origem.dat_envio_sms_1,
            origem.dat_envio_sms_2,
            origem.cod_barra,
            origem.lin_digitavel,
            origem.cod_pix,
            origem.idf_mensagem_mailgun,
            origem.bankslip,
            origem.settlement,
            origem.webhook,
            origem.mla_funcionario,
            origem.dat_cancelamento,
            origem.cod_motivo,
            origem.des_observacao,
            origem.mla_funcionario_cancelamento,
        ))

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
            tabela = 'fatura'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
