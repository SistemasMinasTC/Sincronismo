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

    Chave = recordtype('Chave', 'cod_clube, cod_tipo_associado, cod_cota, cod_banco')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from DebitoAutomatico
            where
                IdDebitoAutomatico = (select PkSql from PkDePara where Tabela = 'DebitoAutomatico' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            cod_banco,
            cod_agencia,
            nro_conta,
            dv_conta,
            cod_operacao,
            cpf_titular_conta,
            dat_ini_autoriza,
            dat_cancelamento
        from {linha_log.banco}:cota_debito_auto as cota_debito_auto
        where
            cod_tipo_associado = ? and
            cod_cota = ?
    """,(
        chave.cod_tipo_associado,
        chave.cod_cota
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update DebitoAutomatico set
            IdCota = (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?),
            IdBanco = ?,
            CodigoAgencia = ?,
            NumeroConta = ?,
            DigitoConta = ?,
            OperacaoConta = ?,
            CPF = ?,
            DataInicio = ?,
            DataCancelamento = ?,
            UltimaAlteracao = getdate()
        where
            IdDebitoAutomatico = (select PkSql from PkDePara where Tabela = 'DebitoAutomatico' and PkIfx = ?)
    """,(
            chave.cod_clube,
            chave.cod_tipo_associado,
            chave.cod_cota,
            origem.cod_banco,
            origem.cod_agencia,
            origem.nro_conta,
            origem.dv_conta,
            origem.cod_operacao,
            origem.cpf_titular_conta,
            origem.dat_ini_autoriza,
            origem.dat_cancelamento,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute(f"""
            insert into DebitoAutomatico
            (
                IdCota,
                IdBanco,
                CodigoAgencia,
                NumeroConta,
                DigitoConta,
                OperacaoConta,
                CPF,
                DataInicio,
                DataCancelamento
            ) values (
                (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?) /*IdCota*/,
                ? /*IdBanco*/,
                ? /*CodigoAgencia*/,
                ? /*NumeroConta*/,
                ? /*DigitoConta*/,
                ? /*OperacaoConta*/,
                ? /*CPF*/,
                ? /*DataInicio*/,
                ? /*DataCancelamento*/
            )
        """,(
            chave.cod_clube,
            chave.cod_tipo_associado,
            chave.cod_cota,
            origem.cod_banco,
            origem.cod_agencia,
            origem.nro_conta,
            origem.dv_conta,
            origem.cod_operacao,
            origem.cpf_titular_conta,
            origem.dat_ini_autoriza,
            origem.dat_cancelamento
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
            tabela = 'cota_debito_auto'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
