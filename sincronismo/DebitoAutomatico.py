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
                IdClube,
                TipoCota,
                NumeroCota
            from Cota
            where IdCota = ?
        """,(
            origem.IdCota,
        ))

        IdClube,TipoCota,NumeroCota = cr_sql.fetchone()

        linha_log.banco = 'minas' if IdClube == 'MTC' else 'nautico'

        cr_ifx.execute(f"""
            delete from {linha_log.banco}:cota_debito_auto
            where
                cod_tipo_associado = ? and
                cod_cota = ?
        """, (
            TipoCota,
            NumeroCota,
        ))

        cr_sql.close()

        return

    cr_sql.execute("""
        select
            Cota.IdClube,
            Cota.TipoCota,
            Cota.NumeroCota,
            DebitoAutomatico.IdBanco,
            DebitoAutomatico.CodigoAgencia,
            DebitoAutomatico.NumeroConta,
            DebitoAutomatico.DigitoConta,
            DebitoAutomatico.CPF,
            DebitoAutomatico.DataInicio,
            DebitoAutomatico.DataCancelamento,
            DebitoAutomatico.OperacaoConta
        from DebitoAutomatico
        inner join Cota on Cota.IdCota = DebitoAutomatico.IdCota
        where
            IdDebitoAutomatico = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.close()

    linha_log.banco = 'minas' if origem.IdClube == 'MTC' else 'nautico'

    cr_ifx.execute(f"""
        update {linha_log.banco}:cota_debito_auto set
            cod_banco = ?,
            cod_agencia = ?,
            nro_conta = ?,
            dv_conta = ?,
            cpf_titular_conta = ?,
            dat_ini_autoriza = ?,
            dat_cancelamento = ?,
            cod_operacao = ?
        where
            cod_tipo_associado = ? and
            cod_cota = ?
    """,(
            origem.IdBanco,
            origem.CodigoAgencia,
            origem.NumeroConta,
            origem.DigitoConta,
            origem.CPF,
            origem.DataInicio,
            origem.DataCancelamento,
            origem.OperacaoConta,

            origem.TipoCota,
            origem.NumeroCota,
    ))

    if cr_ifx.rowcount == 0:
        cr_ifx.execute(f"""
            insert into {linha_log.banco}:cota_debito_auto
            (
                cod_tipo_associado,
                cod_cota,
                cod_banco,
                cod_agencia,
                nro_conta,
                dv_conta,
                cpf_titular_conta,
                dat_ini_autoriza,
                dat_cancelamento,
                cod_operacao
            ) values (
                ? {{cod_tipo_associado}},
                ? {{cod_cota}},
                ? {{cod_banco}},
                ? {{cod_agencia}},
                ? {{nro_conta}},
                ? {{dv_conta}},
                ? {{cpf_titular_conta}},
                ? {{dat_ini_autoriza}},
                ? {{dat_cancelamento}},
                ? {{cod_operacao}}
            )
        """,(
            origem.TipoCota,
            origem.NumeroCota,
            origem.IdBanco,
            origem.CodigoAgencia,
            origem.NumeroConta,
            origem.DigitoConta,
            origem.CPF,
            origem.DataInicio,
            origem.DataCancelamento,
            origem.OperacaoConta
        ))

    # acerta idc_deb_automatico na cota
    #
    cr_ifx.execute(f"""
        update {linha_log.banco}:_cota_ set
            idc_deb_automatico = ?
        where
            cod_tipo_associado = ? and
            cod_cota = ?
    """,(
            'N' if origem.DataCancelamento else 'S',
            origem.TipoCota,
            origem.NumeroCota,
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
            tabela = 'DebitoAutomatico'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)

