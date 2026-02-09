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

    Chave = recordtype('Chave', 'cod_clube, nro_acerto_cancel')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Acerto
            where
                IdAcerto = (select PkSql from PkDePara where Tabela = 'Acerto' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""

        select
            '{cod_clube}|' || acerto_cancel.nro_acerto_cancel as nro_acerto_cancel,
            '{cod_clube}|' || acerto_cancel_rec.cod_tipo_associado || '|' || acerto_cancel_rec.cod_cota as cod_cota,
            dat_acerto_cancel,
            trim
            (
                case
                    when cod_motivo = 'RENEG' then 'Renegociação'
                    when idt_acerto_cancel = 'C' then 'Cancelamento'
                    else 'Baixa'
                end
            ) as cod_tipo_acerto,
            vlr_divida,
            qtd_parcelas,
            vlr_gerado,
            vlr_encargos,
            cod_motivo::varchar(20) as cod_motivo,
            des_observacao,
            (select cod_usuario from acesso:usuario where mla_func = acerto_cancel.mla_funcionario) as cod_usuario,
            (select cod_usuario from acesso:usuario where mla_func = acerto_cancel.mla_func_estorno) as cod_usuario_estorno,
            dat_estorno
        from {linha_log.banco}:acerto_cancel as acerto_cancel
        inner join {linha_log.banco}:acerto_cancel_rec as acerto_cancel_rec on
            acerto_cancel_rec.nro_acerto_cancel = acerto_cancel.nro_acerto_cancel
        left join {linha_log.banco}:cota_reneg_divida as cota_reneg_divida on
            cota_reneg_divida.cod_tipo_associado  = acerto_cancel_rec.cod_tipo_associado and
            cota_reneg_divida.cod_cota  = acerto_cancel_rec.cod_cota and
            cota_reneg_divida.dat_renegociacao = acerto_cancel.dat_acerto_cancel and
            acerto_cancel.cod_motivo = 'RENEG'
        where
            acerto_cancel.nro_acerto_cancel = ?
    """,(
        chave.nro_acerto_cancel,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    origem = Linha(*linha) if (linha := cr_ifx.fetchone()) else None

    cr_sql.execute("""
        update Acerto set
            IdCota = (select PkSql from PkDePara where Tabela = 'Cota' and PkIfx = ?),
            DataAcerto = ?,
            TipoAcerto = ?,
            Valor = ?,
            NumeroParcelasRenegociacao = ?,
            ValorRenegociado = ?,
            ValorEncargos = ?,
            IdMotivo = (select IdMotivo from Motivo where CodigoMotivo = ?),
            Observacao = ?,
            IdUsuario = (select PkSql from PkDePara where Tabela = 'Usuario' and PkIfx = ?),
            IdUsuarioEstorno = (select PkSql from PkDePara where Tabela = 'Usuario' and PkIfx = ?),
            DataEstorno = ?,
            UltimaAlteracao = getdate()
        where
            IdAcerto = (select PkSql from PkDePara where Tabela = 'Acerto' and PkIfx = ?)
    """,(
            origem.cod_cota,
            origem.dat_acerto_cancel,
            origem.cod_tipo_acerto,
            origem.vlr_divida,
            origem.qtd_parcelas,
            origem.vlr_gerado,
            origem.vlr_encargos,
            origem.cod_motivo,
            origem.des_observacao,
            origem.cod_usuario,
            origem.cod_usuario_estorno,
            origem.dat_estorno,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Acerto
            (
                IdCota,
                DataAcerto,
                TipoAcerto,
                Valor,
                NumeroParcelasRenegociacao,
                ValorRenegociado,
                ValorEncargos,
                IdMotivo,
                Observacao,
                IdUsuario,
                IdUsuarioEstorno,
                DataEstorno
            ) values (
                (select PkSql from PkDePara where Tabela = 'Cota' and PkIfx = ?) /*IdCota*/,
                ? /*DataAcerto*/,
                ? /*TipoAcerto*/,
                ? /*Valor*/,
                ? /*NumeroParcelasRenegociacao*/,
                ? /*ValorRenegociado*/,
                ? /*ValorEncargos*/,
                (select IdMotivo from Motivo where CodigoMotivo = ?) /*IdMotivo*/,
                ? /*Observacao*/,
                (select PkSql from PkDePara where Tabela = 'Usuario' and PkIfx = ?) /*IdUsuario*/,
                (select PkSql from PkDePara where Tabela = 'Usuario' and PkIfx = ?) /*IdUsuarioEstorno*/,
                ? /*DataEstorno*/
            )
        """,(
            origem.cod_cota,
            origem.dat_acerto_cancel,
            origem.cod_tipo_acerto,
            origem.vlr_divida,
            origem.qtd_parcelas,
            origem.vlr_gerado,
            origem.vlr_encargos,
            origem.cod_motivo,
            origem.des_observacao,
            origem.cod_usuario,
            origem.cod_usuario_estorno,
            origem.dat_estorno,
        ))

        cr_sql.execute("""select ident_current('Acerto')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Acerto',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'acerto_cancel'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
