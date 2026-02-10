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
            select PkIfx
            from PkDePara
            where
                tabela = 'Acerto' and
                PkSql= ?
        """,(
            origem.IdAcerto,
        ))

        linha_log.banco, nro_acerto_cancel = cr_sql.fetchval().split('|')

        cr_ifx.execute(f"""
            delete from {linha_log.banco}:acerto_cancel
            where
                nro_acerto_cancel
        """, (
            nro_acerto_cancel
        ))

        cr_sql.close()
        return

    cr_sql.execute("""
        select
            Acerto.IdCota,
            cast(Acerto.DataAcerto as date) as DataAcerto,
            Acerto.TipoAcerto, -- necessário para tratar renegociacao
            case Acerto.TipoAcerto
                when 'Cancelamento' then 'C'
                else 'M'
            end as idt_acerto_cancel,
            Acerto.Valor,
            Acerto.NumeroParcelasRenegociacao,
            Acerto.ValorRenegociado,
            Acerto.ValorEncargos,
            Acerto.DevolucaoNoCaixa,
            Acerto.DocumentoQuitacaoDevolucao,
            case when TipoAcerto = 'Renegociação' then 'RENEG' else Motivo.CodigoMotivo end as CodigoMotivo,
            Acerto.Observacao,
            isnull(Usuario.Matricula,0) as Matricula,
            Acerto.UltimaAlteracao,
            cast(Acerto.DataEstorno as date) as DataEstorno,
            UsuarioEstorno.Matricula as MatriculaEstorno,
            Cota.IdClube,
            Cota.TipoCota,
            Cota.NumeroCota,
            PkIfx
        from Acerto
        inner join Cota on Cota.IdCota = Acerto.IdCota
        left join Usuario on Usuario.IdUsuario = Acerto.IdUsuario
        left join Motivo on Motivo.IdMotivo = Acerto.IdMotivo
        left join Usuario as UsuarioEstorno on UsuarioEstorno.IdUsuario = Acerto.IdUsuarioEstorno
        left join PkDePara on
            PkDePara.Tabela = 'Acerto' and
            PkDePara.PkSql = Acerto.IdAcerto
        where
            IdAcerto = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    origem = Linha(*linha) if (linha := cr_sql.fetchone())  else None

    cr_sql.close()

    Chave = recordtype('Chave','cod_clube, nro_acerto_cancel')
    chave = Chave(*origem.PkIfx.split('|')) if origem.PkIfx else Chave(None, None)

    linha_log.banco = 'minas' if origem.IdClube == 'MTC' else 'nautico'

    cr_ifx.execute(f"""
        update {linha_log.banco}:acerto_cancel
        set
            dat_acerto_cancel = ?,
            mla_funcionario = ?,
            idt_acerto_cancel = ?,
            des_observacao = ?,
            cod_motivo = ?,
            dat_estorno = ?,
            mla_func_estorno = ?
        where
            nro_acerto_cancel = ?
    """,(
        origem.DataAcerto,
        origem.Matricula,
        origem.idt_acerto_cancel,
        origem.Observacao[:60] if origem.Observacao else None,
        origem.CodigoMotivo,
        origem.DataEstorno,
        origem.MatriculaEstorno,
        chave.nro_acerto_cancel,
    ))

    if cr_ifx.rowcount == 0:
        cr_ifx.execute(f"""
            insert into {linha_log.banco}:acerto_cancel
            (
                dat_acerto_cancel,
                mla_funcionario,
                idt_acerto_cancel,
                des_observacao,
                cod_motivo,
                dat_estorno,
                mla_func_estorno
            ) values (
                ? {{dat_acerto_cancel}},
                ? {{mla_funcionario}},
                ? {{idt_acerto_cancel}},
                ? {{des_observacao}},
                ? {{cod_motivo}},
                ? {{dat_estorno}},
                ? {{mla_func_estorno}}
            )
        """,(
            origem.DataAcerto,
            origem.Matricula,
            origem.idt_acerto_cancel,
            origem.Observacao[:60] if origem.Observacao else None,
            origem.CodigoMotivo,
            origem.DataEstorno,
            origem.MatriculaEstorno
        ))

        cr_ifx.execute("""select dbinfo('sqlca.sqlerrd1') from dual""")
        origem.PkIfx = f"{origem.IdClube}|{cr_ifx.fetchone()[0]}"

        cr_sql = conn_sql.cursor()
        cr_sql.execute("delete from PkDePara where Tabela = 'Acerto' and PkSql = ?",(linha_log.pk,))
        cr_sql.execute("insert into PkDePara values ('Acerto',?,?)",(linha_log.pk, origem.PkIfx,))
        cr_sql.close

    # Trata cota_reneg_divida
    #
    if origem.TipoAcerto == 'Renegociação':
        cr_ifx.execute(f"""
            update {linha_log.banco}:cota_reneg_divida
            set
                qtd_parcelas = ?,
                vlr_divida = ?,
                vlr_encargos = ?,
                vlr_gerado = ?,
                mla_funcionario = ?
            where
                cod_tipo_associado = ? and
                cod_cota = ? and
                dat_renegociacao = ?
        """,(
            origem.NumeroParcelasRenegociacao,
            origem.Valor,
            origem.ValorEncargos,
            origem.ValorRenegociado,
            origem.Matricula,

            origem.TipoCota,
            origem.NumeroCota,
            origem.DataAcerto
        ))

        if cr_sql.rowcount == 0:
            cr_ifx.execute(f"""
                insert into {linha_log.banco}:cota_reneg_divida
                (
                    cod_tipo_associado,
                    cod_cota,
                    dat_renegociacao,
                    qtd_parcela,
                    vlr_divida,
                    vlr_encargos,
                    vlr_gerado,
                    mla_funcionario
                ) values (
                    ? {{cod_tipo_associado}},
                    ? {{cod_cota}},
                    ? {{dat_renegociacao}},
                    ? {{qtd_parcelas}},
                    ? {{vlr_divida}},
                    ? {{vlr_encargos }},
                    ? {{vlr_gerado }},
                    ? {{mla_funcionario}}
            """,(
                origem.TipoCota,
                origem.NumeroCota,
                origem.DataAcerto,
                origem.NumeroParcelasRenegociacao,
                origem.Valor,
                origem.ValorEncargos,
                origem.ValorRenegociado,
                origem.Matricula,
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
            tabela = 'Acerto'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)

