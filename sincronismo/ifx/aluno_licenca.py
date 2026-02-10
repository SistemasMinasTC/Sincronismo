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

    Chave = recordtype('Chave', 'cod_clube, cod_associado, cod_curso, cod_turma, dat_inicio_licenca')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from LicencaMedica
            where
                IdLicencaMedica = (select PkSql from PkDePara where Tabela = 'LicencaMedica' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.execute(f"""
            delete from PkDePara
            where
                Tabela = 'LicencaMedica' and
                PkSql = (select PkSql from PkDePara where Tabela = 'LicencaMedica' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}|' || cod_associado || '|' || cod_curso || '|' || cod_turma as cod_aluno,
            dat_inclusao,
            dat_inicio_licenca,
            dat_fim_licenca,
            des_observacao
        from {linha_log.banco}:aluno_licenca as aluno_licenca
        where
            cod_associado = ? and
            cod_curso = ? and
            cod_turma = ? and
            dat_inicio_licenca = to_date(?,'%d/%m/%Y')
    """,(
        chave.cod_associado,
        chave.cod_curso,
        chave.cod_turma,
        chave.dat_inicio_licenca,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update LicencaMedica set
            IdAluno = (select PkSql from PkDePara where Tabela = 'Aluno' and PkIfx = ?),
            DataInclusao = ?,
            DataInicio = ?,
            DataFim = ?,
            Observacao = ?,
            UltimaAlteracao = getdate()
        where
            IdLicencaMedica = (select PkSql from PkDePara where Tabela = 'LicencaMedica' and PkIfx = ?)
    """,(
            origem.cod_aluno,
            origem.dat_inclusao,
            origem.dat_inicio_licenca,
            origem.dat_fim_licenca,
            origem.des_observacao,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into LicencaMedica
            (
                IdAluno,
                DataInclusao,
                DataInicio,
                DataFim,
                Observacao
            ) values (
                (select PkSql from PkDePara where Tabela = 'Aluno' and PkIfx = ?) /*IdAluno*/,
                ? /*DataInclusao*/,
                ? /*DataInicio*/,
                ? /*DataFim*/,
                ? /*Observacao*/
            )
        """,(
            origem.cod_aluno,
            origem.dat_inclusao,
            origem.dat_inicio_licenca,
            origem.dat_fim_licenca,
            origem.des_observacao
        ))

        cr_sql.execute("""select ident_current('LicencaMedica')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('LicencaMedica',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'aluno_licenca'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
