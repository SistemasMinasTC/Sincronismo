#!/usr/bin/python
#

from recordtype import recordtype
from datetime import datetime

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
    elif linha_log.banco == 'serra':
        cod_clube = 'MSDR'

    linha_log.pk = cod_clube+'|'+linha_log.pk

    Chave = recordtype('Chave', 'cod_clube, cod_associado, cod_curso, cod_turma, dat_inicio_licenca')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute("""
            delete from AlunoLicenca
            where
                IdAluno = (
                    select Aluno.IdAluno
                    from Aluno
                    inner join Associado on Associado.IdAssociado = Aluno.IdAssociado
                    inner join Turma on Turma.IdTurma = Aluno.IdTurma
                    inner join Curso on Curso.IdCurso = Turma.IdCurso
                    where
                        Curso.IdClube = ? and
                        Associado.NPF = ? and
                        Curso.CodigoCurso = ? and
                        Turma.CodigoTurma = ?
                ) and
                DataInicio = ?
        """, (
            chave.cod_clube,
            chave.cod_associado,
            chave.cod_curso,
            chave.cod_turma,
            datetime.strptime(chave.dat_inicio_licenca, "%d/%m/%Y").date(),
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            aluno_licenca.cod_associado, 
            nvl(ped_transf.cod_curso_transf, aluno_licenca.cod_curso) as cod_curso, 
            nvl(ped_transf.cod_turma_transf, aluno_licenca.cod_turma) as cod_turma, 
            aluno_licenca.dat_inclusao,
            aluno_licenca.dat_inicio_licenca,
            aluno_licenca.dat_fim_licenca,
            aluno_licenca.des_observacao
        from {linha_log.banco}:aluno_licenca as aluno_licenca
        left join {linha_log.banco}:ped_transf as ped_transf on
            ped_transf.cod_associado = aluno_licenca.cod_associado and
            ped_transf.cod_curso = aluno_licenca.cod_curso and
            ped_transf.cod_turma = aluno_licenca.cod_turma
        where
            aluno_licenca.cod_associado = ? and
            aluno_licenca.cod_curso = ? and
            aluno_licenca.cod_turma = ? and
            aluno_licenca.dat_inicio_licenca = to_date(?,'%d/%m/%Y')
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
        update AlunoLicenca set
            DataInclusao = ?,
            DataFim = ?,
            Observacao = ?,
            UltimaAlteracao = getdate()
        where
            IdAluno = (
                select Aluno.IdAluno
                from Aluno
                inner join Associado on Associado.IdAssociado = Aluno.IdAssociado
                inner join Turma on Turma.IdTurma = Aluno.IdTurma
                inner join Curso on Curso.IdCurso = Turma.IdCurso
                where
                    Curso.IdClube = ? and
                    Associado.NPF = ? and
                    Curso.CodigoCurso = ? and
                    Turma.CodigoTurma = ?
            ) and
            DataInicio = ?
    """,(
            origem.dat_inclusao,
            origem.dat_fim_licenca,
            origem.des_observacao,
            chave.cod_clube,
            origem.cod_associado,
            origem.cod_curso,
            origem.cod_turma,
            origem.dat_inicio_licenca,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute("""
            insert into AlunoLicenca
            (
                IdAluno,
                DataInclusao,
                DataInicio,
                DataFim,
                Observacao
            ) values (
                (
                    select Aluno.IdAluno
                    from Aluno
                    inner join Associado on Associado.IdAssociado = Aluno.IdAssociado
                    inner join Turma on Turma.IdTurma = Aluno.IdTurma
                    inner join Curso on Curso.IdCurso = Turma.IdCurso
                    where
                        Curso.IdClube = ? and
                        Associado.NPF = ? and
                        Curso.CodigoCurso = ? and
                        Turma.CodigoTurma = ?
                ) /*IdAluno*/,
                ? /*DataInclusao*/,
                ? /*DataInicio*/,
                ? /*DataFim*/,
                ? /*Observacao*/
            )
        """,(
            chave.cod_clube,
            origem.cod_associado,
            origem.cod_curso,
            origem.cod_turma,
            origem.dat_inclusao,
            origem.dat_inicio_licenca,
            origem.dat_fim_licenca,
            origem.des_observacao
        ))

        cr_sql.execute("commit transaction")

    cr_sql.close()

# Teste
#
if __name__ == "__main__":
    import sys
    from conexoes import *

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
            tabela = 'aluno_licenca' and tentativas = 1957
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
