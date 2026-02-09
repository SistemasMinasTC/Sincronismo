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

    Chave = recordtype('Chave', 'cod_clube, cod_curso')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Curso
            where
                IdCurso = (select PkSql from PkDePara where Tabela = 'Curso' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            cod_curso,
            '{cod_clube}|' || cod_mod_curso as cod_mod_curso,
            des_curso,
            txt_app,
            idc_exige_atestado = 'S' as idc_exige_atestado,
            idc_exige_parq = 'S' as idc_exige_parq,
            idc_mla_isenta = 'S' as idc_mla_isenta,
            idc_mla_padrao = 'S' as idc_mla_padrao,
            per_desc_mla,
            idc_terceirizado = 'S' as idc_terceirizado,
            des_local_terc,
            idc_curso_temp = 'S' as idc_curso_temp,
            dat_ini_curso_temp,
            dat_fim_curso_temp,
            '{cod_clube}|' || cod_plano as cod_plano,
            idc_incide_desc = 'S' as idc_incide_desc,
            idc_paga_janeiro = 'S' as idc_paga_janeiro,
            idc_paga_fevereiro = 'S' as idc_paga_fevereiro,
            idc_paga_julho = 'S' as idc_paga_julho,
            idc_paga_dezembro = 'S' as idc_paga_dezembro,
            dat_desativacao,
            idc_licenca_medica = 'S' as idc_licenca_medica,
            idc_reserva_vaga = 'S' as idc_reserva_vaga,
            idc_mensal_propor = 'S' as idc_mensal_propor,
            idc_valor_cancel = 'S' as idc_valor_cancel,
            vlr_taxa_especial,
            cod_subarea,
            (select cod_usuario from acesso:usuario where idf_cidadao = curso.idf_cidadao_desat) as idf_cidadao_desat
        from {linha_log.banco}:curso as curso
        where
            cod_curso = ?
    """,(
        chave.cod_curso,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Curso set
            IdClube = ?,
            CodigoCurso = ?,
            IdModalidadeEsportiva = (select PkSql from PkDePara where Tabela = 'ModalidadeEsportiva' and PkIfx = ?),
            NomeCurso = ?,
            TextoApp = ?,
            ExigeAtestado = ?,
            ExigeParq = ?,
            MatriculaIsenta = ?,
            MatriculaPadrao = ?,
            PercentualDescontoMatricula = ?,
            Terceirizado = ?,
            LocalTerceiro = ?,
            Temporario = ?,
            DataInicio = ?,
            DataFim = ?,
            IdPlanoCobranca = (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?),
            Desconto = ?,
            PagaJaneiro = ?,
            PagaFevereiro = ?,
            PagaJulho = ?,
            PagaDezembro = ?,
            DataDesativacao = ?,
            PermiteLicencaMedica = ?,
            PermiteLicencaReserva = ?,
            PermiteMensalidadeProporcional = ?,
            CobraValorCancelamento = ?,
            TaxaEspecial = ?,
            IdSubArea = (select PkSql from PkDePara where Tabela = 'SubArea' and PkIfx = ?),
            IdUsuario = (select PkSql from PkDePara where Tabela = 'Usuario' and PkIfx = ?),
            UltimaAlteracao = getdate()
        where
            IdCurso = (select PkSql from PkDePara where Tabela = 'Curso' and PkIfx = ?)
    """,(
            origem.cod_clube,
            origem.cod_curso,
            origem.cod_mod_curso,
            origem.des_curso,
            origem.txt_app,
            origem.idc_exige_atestado,
            origem.idc_exige_parq,
            origem.idc_mla_isenta,
            origem.idc_mla_padrao,
            origem.per_desc_mla,
            origem.idc_terceirizado,
            origem.des_local_terc,
            origem.idc_curso_temp,
            origem.dat_ini_curso_temp,
            origem.dat_fim_curso_temp,
            origem.cod_plano,
            origem.idc_incide_desc,
            origem.idc_paga_janeiro,
            origem.idc_paga_fevereiro,
            origem.idc_paga_julho,
            origem.idc_paga_dezembro,
            origem.dat_desativacao,
            origem.idc_licenca_medica,
            origem.idc_reserva_vaga,
            origem.idc_mensal_propor,
            origem.idc_valor_cancel,
            origem.vlr_taxa_especial,
            origem.cod_subarea,
            origem.idf_cidadao_desat,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Curso
            (
                IdClube,
                CodigoCurso,
                IdModalidadeEsportiva,
                NomeCurso,
                TextoApp,
                ExigeAtestado,
                ExigeParq,
                MatriculaIsenta,
                MatriculaPadrao,
                PercentualDescontoMatricula,
                Terceirizado,
                LocalTerceiro,
                Temporario,
                DataInicio,
                DataFim,
                IdPlanoCobranca,
                Desconto,
                PagaJaneiro,
                PagaFevereiro,
                PagaJulho,
                PagaDezembro,
                DataDesativacao,
                PermiteLicencaMedica,
                PermiteLicencaReserva,
                PermiteMensalidadeProporcional,
                CobraValorCancelamento,
                TaxaEspecial,
                IdSubArea,
                IdUsuario
            ) values (
                ? /*IdClube*/,
                ? /*CodigoCurso*/,
                (select PkSql from PkDePara where Tabela = 'ModalidadeEsportiva' and PkIfx = ?) /*IdModalidadeEsportiva*/,
                ? /*NomeCurso*/,
                ? /*TextoApp*/,
                ? /*ExigeAtestado*/,
                ? /*ExigeParq*/,
                ? /*MatriculaIsenta*/,
                ? /*MatriculaPadrao*/,
                ? /*PercentualDescontoMatricula*/,
                ? /*Terceirizado*/,
                ? /*LocalTerceiro*/,
                ? /*Temporario*/,
                ? /*DataInicio*/,
                ? /*DataFim*/,
                (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?) /*IdPlanoCobranca*/,
                ? /*Desconto*/,
                ? /*PagaJaneiro*/,
                ? /*PagaFevereiro*/,
                ? /*PagaJulho*/,
                ? /*PagaDezembro*/,
                ? /*DataDesativacao*/,
                ? /*PermiteLicencaMedica*/,
                ? /*PermiteLicencaReserva*/,
                ? /*PermiteMensalidadeProporcional*/,
                ? /*CobraValorCancelamento*/,
                ? /*TaxaEspecial*/,
                (select PkSql from PkDePara where Tabela = 'SubArea' and PkIfx = ?) /*IdSubArea*/,
                (select PkSql from PkDePara where Tabela = 'Usuario' and PkIfx = ?) /*IdUsuario*/
            )
        """,(
            origem.cod_clube,
            origem.cod_curso,
            origem.cod_mod_curso,
            origem.des_curso,
            origem.txt_app,
            origem.idc_exige_atestado,
            origem.idc_exige_parq,
            origem.idc_mla_isenta,
            origem.idc_mla_padrao,
            origem.per_desc_mla,
            origem.idc_terceirizado,
            origem.des_local_terc,
            origem.idc_curso_temp,
            origem.dat_ini_curso_temp,
            origem.dat_fim_curso_temp,
            origem.cod_plano,
            origem.idc_incide_desc,
            origem.idc_paga_janeiro,
            origem.idc_paga_fevereiro,
            origem.idc_paga_julho,
            origem.idc_paga_dezembro,
            origem.dat_desativacao,
            origem.idc_licenca_medica,
            origem.idc_reserva_vaga,
            origem.idc_mensal_propor,
            origem.idc_valor_cancel,
            origem.vlr_taxa_especial,
            origem.cod_subarea,
            origem.idf_cidadao_desat
        ))

        cr_sql.execute("""select ident_current('Curso')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Curso',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'curso'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
