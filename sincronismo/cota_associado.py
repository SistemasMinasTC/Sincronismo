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

    Chave = recordtype('Chave', 'cod_clube, cod_associado, cod_tipo_associado, cod_cota')
    chave = Chave(*linha_log.pk.split('|'))

    # Busca dados do associado em ordem da última admissão
    #
    cr_sql.execute("""
        select Associado.IdAssociado
        from Associado
        inner join Cota on Cota.IdCota = Associado.IdCota
        where
           Cota.IdClube = ? and
           Associado.NPF = ? and
           Cota.TipoCota = ? and
           Cota.NumeroCota = ?
        order by DataAdmissao desc
    """, (
        chave.cod_clube,
        chave.cod_associado,
        chave.cod_tipo_associado,
        chave.cod_cota
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    dados = Linha(*linha) if (linha := cr_sql.fetchone()) else None

    if linha_log.operacao == 'del':
        if dados:
            cr_ifx.execute("""
                select nvl(max(dat_movto),today) as dat_exclusao
                from movto_associado
                where
                    cod_movto in ('EXC','EXT','EXI','ECO','EXV') and
                    cod_associado = ? and
                    cod_tipo_associado = ? and
                    cod_cota = ?
            """, (
                    chave.cod_associado,
                    chave.cod_tipo_associado,
                    chave.cod_cota,
            ))

            dat_exclusao = cr_ifx.fetchone()[0]

            cr_sql.execute(f"""
                update Associado
                set
                    DataExclusao = ?
                where
                    IdAssociado = ?
            """, (
                dat_exclusao,
                dados.IdAssociado,
            ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            cod_tipo_associado,
            cod_cota,
            'Associado|'||cota_associado.cod_associado as pk_associado,
            cota_associado.cod_associado,
            '{cod_clube}'||'|'||cod_vinculo(cod_parentesco) as cod_vinculo,
            associado.mla_funcionario_associado,
            cota_associado.dat_incl_associado,
            associado.cod_unimed,
            associado.cod_contr_unimed,
            associado.cod_paren_unimed,
            associado.dat_inc_unimed,
            associado.vlr_limite_compra,
            associado.des_token,
            associado.dat_validade_token,
            associado.des_token_mywellness,
            associado.idc_acompanhante = 'S' as idc_acompanhante,
            associado.idc_menor_liberado = 'S' as idc_menor_liberado,
            associado.idc_compra_ingresso = 'S' as idc_compra_ingresso,
            associado.idc_termo_acesso = 'S' as idc_termo_acesso,
            associado.cod_carteira,
            associado.dat_exclusao_comunicacao,
            associado.dat_exclusao_parceiro,
            case cota_associado.idt_status
               when 'N' then 'Normal'
               when 'P' then 'Processo'
            end as idt_status,
            cota_associado.cod_tipo_associado = cod_tipo_prior and cod_cota = cod_cota_prior as idt_prioritaria,
            pessoa_fisica.idc_primeira_vez = 'N' as idc_primeira_vez,
            (select cod_tipo_restricao from {linha_log.banco}:pessoa_fisica as pessoa_fisica where idt_pessoa = 1 and cod_pessoa = cota_associado.cod_associado) as cod_restricao
        from {linha_log.banco}:cota_associado as cota_associado
        inner join {linha_log.banco}:associado as associado on
            associado.cod_associado = cota_associado.cod_associado
        left join {linha_log.banco}:pessoa_fisica as pessoa_fisica on
            pessoa_fisica.idt_pessoa = 1 and
            pessoa_fisica.cod_pessoa = associado.cod_associado
        where
            cota_associado.cod_associado = ? and
            cod_tipo_associado = ? and
            cod_cota = ?
    """,(
        chave.cod_associado,
        chave.cod_tipo_associado,
        chave.cod_cota,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    origem = Linha(*linha) if (linha := cr_ifx.fetchone()) else None

    if not origem:
        cr_sql.close()
        return

    if dados:
        cr_sql.execute(f"""
            update Associado set
                IdVinculo = (select PkSql from PkDePara where Tabela = 'Vinculo' and PkIfx = ?),
                MatriculaFuncionario = ?,
                DataAdmissao = ?,
                CodigoUnimed = ?,
                CodigoContratoUnimed = ?,
                CodigoVinculoUnimed = ?,
                DataInclusaoUnimed = ?,
                ValorLimiteCompra = ?,
                Token = ?,
                ValidadeToken = ?,
                TokenMywellness = ?,
                PrecisaAcompanhante = ?,
                MenorLiberado = ?,
                CompraIngresso = ?,
                TermoAdesao = ?,
                CodigoCarteira = ?,
                DataExclusaoComunicacao = ?,
                DataExclusaoParceiro = ?,
                Status = ?,
                Prioritario = ?,
                AcessouEmAtraso = ?,
                CodigoRestricao = ?,
                DataExclusao = null,
                UltimaAlteracao = getdate()
            where
                IdAssociado = ?
        """,(
                origem.cod_vinculo,
                origem.mla_funcionario_associado,
                origem.dat_incl_associado,
                origem.cod_unimed,
                origem.cod_contr_unimed,
                origem.cod_paren_unimed,
                origem.dat_inc_unimed,
                origem.vlr_limite_compra,
                origem.des_token,
                origem.dat_validade_token,
                origem.des_token_mywellness,
                origem.idc_acompanhante,
                origem.idc_menor_liberado,
                origem.idc_compra_ingresso,
                origem.idc_termo_acesso,
                origem.cod_carteira,
                origem.dat_exclusao_comunicacao,
                origem.dat_exclusao_parceiro,
                origem.idt_status,
                origem.idt_prioritaria,
                origem.idc_primeira_vez,
                origem.cod_restricao,

                dados.IdAssociado,
        ))

    else:
        cr_sql.execute(f"""
            insert into Associado
            (
                IdCota,
                IdPessoa,
                NPF,
                IdVinculo,
                MatriculaFuncionario,
                DataAdmissao,
                CodigoUnimed,
                CodigoContratoUnimed,
                CodigoVinculoUnimed,
                DataInclusaoUnimed,
                ValorLimiteCompra,
                Token,
                ValidadeToken,
                TokenMywellness,
                PrecisaAcompanhante,
                MenorLiberado,
                CompraIngresso,
                TermoAdesao,
                CodigoCarteira,
                DataExclusaoComunicacao,
                DataExclusaoParceiro,
                Status,
                Prioritario,
                AcessouEmAtraso,
                CodigoRestricao
            )  values (
                (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?) /*IdCota*/,
                (select PkSql from PkDePara where Tabela = 'Pessoa' and PkIfx = ?) /*IdPessoa*/,
                ? /*NPF*/,
                (select PkSql from PkDePara where Tabela = 'Vinculo' and PkIfx = ?) /*IdVinculo*/,
                ? /*MatriculaFuncionario*/,
                ? /*DataAdmissao*/,
                ? /*CodigoUnimed*/,
                ? /*CodigoContratoUnimed*/,
                ? /*CodigoVinculoUnimed*/,
                ? /*DataInclusaoUnimed*/,
                ? /*ValorLimiteCompra*/,
                ? /*Token*/,
                ? /*ValidadeToken*/,
                ? /*TokenMywellness*/,
                ? /*PrecisaAcompanhante*/,
                ? /*MenorLiberado*/,
                ? /*CompraIngresso*/,
                ? /*TermoAdesao*/,
                ? /*CodigoCarteira*/,
                ? /*DataExclusaoComunicacao*/,
                ? /*DataExclusaoParceiro*/,
                ? /*Status*/,
                ? /*Prioritario*/,
                ? /*AcessouEmAtraso*/,
                ? /*CodigoRestricao*/
            )
        """,(
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.cod_cota,
            origem.pk_associado,
            origem.cod_associado,
            origem.cod_vinculo,
            origem.mla_funcionario_associado,
            origem.dat_incl_associado,
            origem.cod_unimed,
            origem.cod_contr_unimed,
            origem.cod_paren_unimed,
            origem.dat_inc_unimed,
            origem.vlr_limite_compra,
            origem.des_token,
            origem.dat_validade_token,
            origem.des_token_mywellness,
            origem.idc_acompanhante,
            origem.idc_menor_liberado,
            origem.idc_compra_ingresso,
            origem.idc_termo_acesso,
            origem.cod_carteira,
            origem.dat_exclusao_comunicacao,
            origem.dat_exclusao_parceiro,
            origem.idt_status,
            origem.idt_prioritaria,
            origem.idc_primeira_vez,
            origem.cod_restricao,
        ))

    if origem.idt_prioritaria:
        cr_sql.execute("""
            update Associado
            set
               Prioritario = 0
            where
               IdAssociado in
               (
                    select IdAssociado from Associado
                    inner join Cota on Cota.IdCota = Associado.IdCota
                    where
                        Cota.IdClube = ?  and
                        Associado.NPF = ? and
                        Cota.IdCota <> (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?)
                )
        """,(
            origem.cod_clube,
            origem.cod_associado,
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.cod_cota,
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
            tabela = 'cota_associado' and
            banco = 'nautico' and
            tentativas = 3 and
            pk matches '*QT|3434'
        order by data_hora
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
