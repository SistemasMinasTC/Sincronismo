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

    Chave = recordtype('Chave', 'cod_clube, cod_mod_curso')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from ModalidadeEsportiva
            where
                IdModalidadeEsportiva = (select PkSql from PkDePara where Tabela = 'ModalidadeEsportiva' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            des_mod_curso,
            idc_poliesp = 'S' as idc_poliesp,
            idc_exige_uniforme = 'S' as idc_exige_uniforme,
            idc_exige_avalia = 'S' as idc_exige_avalia,
            idc_exige_agenda = 'S' as idc_exige_agenda,
            idc_academia = 'S' as idc_academia,
            idc_agenda_primeira_aula = 'S' as idc_agenda_primeira_aula,
            idc_coletivo = 'S' as idc_coletivo,
            qtd_dias_aniv,
            idc_idade_acima = 'S' as idc_idade_acima,
            '{cod_clube}|' || cod_rec_avulsa as cod_rec_avulsa,
            txt_pontuacao,
            txt_criterios
        from {linha_log.banco}:modalidade as modalidade
        where
            cod_mod_curso = ?
    """,(
        chave.cod_mod_curso,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update ModalidadeEsportiva set
            IdClube = ?,
            Nome = ?,
            Poliesportiva = ?,
            ExigeUniforme = ?,
            ExigeAvaliacao = ?,
            ExigeAgenda = ?,
            Academia = ?,
            AgendamentoPrimeiraAula = ?,
            Coletiva = ?,
            QuantidadeDiasAniversario = ?,
            PermiteIdadeMaior = ?,
            IdReceitaAvulsa = (select PkSql from PkDePara where Tabela = 'Receita' and PkIfx = ?),
            TextoPontuacao = ?,
            TextoCriterios = ?,
            UltimaAlteracao = getdate()
        where
            IdModalidadeEsportiva = (select PkSql from PkDePara where Tabela = 'ModalidadeEsportiva' and PkIfx = ?)
    """,(
            origem.cod_clube,
            origem.des_mod_curso,
            origem.idc_poliesp,
            origem.idc_exige_uniforme,
            origem.idc_exige_avalia,
            origem.idc_exige_agenda,
            origem.idc_academia,
            origem.idc_agenda_primeira_aula,
            origem.idc_coletivo,
            origem.qtd_dias_aniv,
            origem.idc_idade_acima,
            origem.cod_rec_avulsa,
            origem.txt_pontuacao,
            origem.txt_criterios,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into ModalidadeEsportiva
            (
                IdClube,
                Nome,
                Poliesportiva,
                ExigeUniforme,
                ExigeAvaliacao,
                ExigeAgenda,
                Academia,
                AgendamentoPrimeiraAula,
                Coletiva,
                QuantidadeDiasAniversario,
                PermiteIdadeMaior,
                IdReceitaAvulsa,
                TextoPontuacao,
                TextoCriterios
            ) values (
                ? /*IdClube*/,
                ? /*Nome*/,
                ? /*Poliesportiva*/,
                ? /*ExigeUniforme*/,
                ? /*ExigeAvaliacao*/,
                ? /*ExigeAgenda*/,
                ? /*Academia*/,
                ? /*AgendamentoPrimeiraAula*/,
                ? /*Coletiva*/,
                ? /*QuantidadeDiasAniversario*/,
                ? /*PermiteIdadeMaior*/,
                (select PkSql from PkDePara where Tabela = 'Receita' and PkIfx = ?) /*IdReceitaAvulsa*/,
                ? /*TextoPontuacao*/,
                ? /*TextoCriterios*/
            )
        """,(
            origem.cod_clube,
            origem.des_mod_curso,
            origem.idc_poliesp,
            origem.idc_exige_uniforme,
            origem.idc_exige_avalia,
            origem.idc_exige_agenda,
            origem.idc_academia,
            origem.idc_agenda_primeira_aula,
            origem.idc_coletivo,
            origem.qtd_dias_aniv,
            origem.idc_idade_acima,
            origem.cod_rec_avulsa,
            origem.txt_pontuacao,
            origem.txt_criterios
        ))

        cr_sql.execute("""select ident_current('ModalidadeEsportiva')""")

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('ModalidadeEsportiva',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'modalidade'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
        #try:
        #    convert(ifx, sql, linha)
        #except Exception as erro:
        #    print(erro)
