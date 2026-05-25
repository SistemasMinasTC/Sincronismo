#!/usr/bin/python
#

import sys
import os
from recordtype import recordtype
from google.cloud import storage


def convert(conn_ifx, conn_sql, linha_log):
    credencial = "/usr/local/etc/CredenciaisMinasTenis/CredencialGCP.json" if os.name == 'posix' else "C:\\CredenciaisMinasTenis\\CredencialGCP.json"
    storage_client = storage.Client.from_service_account_json(credencial)
    bucket = storage_client.bucket('minascorp')

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

    linha_log.pk = cod_clube + '|' + linha_log.pk

    Chave = recordtype('Chave', 'cod_clube, cod_associado, cod_curso, cod_turma')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute("""
            update Aluno set
                DataCancelamento = getdate()
            where IdAluno = (
                select Aluno.IdAluno
                from Aluno
                inner join Associado on Associado.IdAssociado = Aluno.IdAssociado
                inner join Turma on Turma.IdTurma         = Aluno.IdTurma
                inner join Curso on Curso.IdCurso         = Turma.IdCurso
                where
                    Curso.IdClube = ? and
                    Associado.NPF = ? and
                    Curso.CodigoCurso = ? and
                    Turma.CodigoTurma = ?
            )
        """, (
            chave.cod_clube,
            chave.cod_associado,
            chave.cod_curso,
            chave.cod_turma,
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            aluno.cod_associado,
            aluno.cod_tipo_associado,
            aluno.cod_cota,
            nvl(ped_transf.cod_curso_transf, aluno.cod_curso) as cod_curso,
            nvl(ped_transf.cod_turma_transf, aluno.cod_turma) as cod_turma,
            aluno.cod_curso as cod_curso_original,
            aluno.cod_turma as cod_turma_original,
            dat_matricula,
            tip_classe_pagto = 'I' as isento,
            dat_pagto_mla,
            dat_recebeu_uniforme,
            dat_fim_day_use,
            aluno.img_parq
        from {linha_log.banco}:aluno as aluno
        left join {linha_log.banco}:ped_transf as ped_transf on
            ped_transf.cod_associado = aluno.cod_associado and
            ped_transf.cod_curso = aluno.cod_curso     and
            ped_transf.cod_turma = aluno.cod_turma
        where
            aluno.cod_associado = ? and
            aluno.cod_curso = ? and
            aluno.cod_turma = ?
    """, (
        chave.cod_associado,
        chave.cod_curso,
        chave.cod_turma,
    ))

    Linha = recordtype('Linha', [col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    if not origem:
        raise Exception('Aluno não encontrado no Informix')

    # Busca IdAssociado pela chave natural
    cr_sql.execute("""
        select top 1 Associado.IdAssociado
        from Associado
        inner join Cota on Cota.IdCota = Associado.IdCota
        where
            Cota.IdClube = ? and
            Associado.NPF = ? and
            Cota.TipoCota = ? and
            Cota.NumeroCota = ?
        order by Associado.DataAdmissao desc
    """, (
        origem.cod_clube,
        origem.cod_associado,
        origem.cod_tipo_associado,
        origem.cod_cota,
    ))

    Linha = recordtype('Linha', [col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    dados = Linha(*linha) if linha else None

    if not dados:
        raise Exception('Associado não encontrado no MinasCorporativo')

    cr_sql.execute("""
        update Aluno set
            IdAssociado = ?,
            IdTurma = (
                select Turma.IdTurma
                from Turma
                inner join Curso on Curso.IdCurso = Turma.IdCurso
                where
                    Curso.IdClube = ? and
                    Curso.CodigoCurso = ? and
                    Turma.CodigoTurma = ?
            ),
            DataMatricula = ?,
            Isento = ?,
            DataPagamentoMatricula = ?,
            DataRecebimentoUniforme = ?,
            DataFimDayUse = ?,
            IdTurmaOriginal = (
                select Turma.IdTurma
                from Turma
                inner join Curso on Curso.IdCurso = Turma.IdCurso
                where
                    Curso.IdClube = ? and
                    Curso.CodigoCurso = ? and
                    Turma.CodigoTurma = ?
            ),
            DataCancelamento = null,
            UltimaAlteracao = getdate()
        where IdAluno = (
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
        )
    """, (
        dados.IdAssociado,
        origem.cod_clube, origem.cod_curso, origem.cod_turma, # IdTurma
        origem.dat_matricula,
        origem.isento,
        origem.dat_pagto_mla,
        origem.dat_recebeu_uniforme,
        origem.dat_fim_day_use,
        origem.cod_clube, origem.cod_curso_original, origem.cod_turma_original, # IdTurmaOriginal
        chave.cod_clube, chave.cod_associado, chave.cod_curso, chave.cod_turma, # where IdAluno
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute("""
            insert into Aluno
            (
                IdAssociado,
                IdTurma,
                DataMatricula,
                Isento,
                DataPagamentoMatricula,
                DataRecebimentoUniforme,
                DataFimDayUse,
                IdTurmaOriginal
            ) values (
                ?,
                (
                    select Turma.IdTurma
                    from Turma
                    inner join Curso on Curso.IdCurso = Turma.IdCurso
                    where
                        Curso.IdClube = ? and
                        Curso.CodigoCurso = ? and
                        Turma.CodigoTurma = ?
                ),
                ?, ?, ?, ?, ?, 
                (
                    select Turma.IdTurma
                    from Turma
                    inner join Curso on Curso.IdCurso = Turma.IdCurso
                    where
                        Curso.IdClube = ? and
                        Curso.CodigoCurso = ? and
                        Turma.CodigoTurma = ?
                )
            )
        """, (
            dados.IdAssociado,
            origem.cod_clube, origem.cod_curso, origem.cod_turma, # IdTurma
            origem.dat_matricula,
            origem.isento,
            origem.dat_pagto_mla,
            origem.dat_recebeu_uniforme,
            origem.dat_fim_day_use,
            origem.cod_clube, origem.cod_curso_original, origem.cod_turma_original, # IdTurmaOriginal
        ))

        cr_sql.execute("select ident_current('Aluno')")
        id_aluno = cr_sql.fetchval()

        cr_sql.execute('commit transaction')
    else:
        cr_sql.execute("""
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
        """, (
            chave.cod_clube,
            chave.cod_associado,
            chave.cod_curso,
            chave.cod_turma,
        ))
        id_aluno = cr_sql.fetchval()

    # Upload da imagem direto para o GCS (sem salvar em disco)
    if origem.img_parq:
        blob = bucket.blob(f"ParqAluno/{id_aluno}.jpg")
        blob.upload_from_string(
            origem.img_parq,
            content_type='image/jpeg'
        )

    cr_sql.close()


# Teste
#
if __name__ == "__main__":
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
            tabela = 'aluno'
        order by data_hora
    """)
    Linha = recordtype('Linha', [col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
