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
    elif linha_log.banco == 'serra':
        cod_clube = 'MSDR'

    linha_log.pk = cod_clube+'|'+linha_log.pk

    Chave = recordtype('Chave', 'cod_clube, cod_curso, cod_turma')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute("""
            delete from HorarioTurma
            where
                IdTurma = (select PkSql from PkDePara where Tabela = 'Turma' and PkIfx = ?)
        """, (
            linha_log.pk
        ))
        
        cr_sql.execute("""
            delete from Turma
            where
                IdTurma = (select PkSql from PkDePara where Tabela = 'Turma' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube, 
            cod_curso,
            cod_turma,
            trim(des_turma) as des_turma,
            cod_nivel,
            cod_unidade, 
            nro_seq_local,
            idc_competicao = 'S' as idc_competicao,
            nro_vagas,
            case idt_sexo
                when 'M' then 'Masculino'
                when 'F' then 'Feminino'
                when 'A' then 'Ambos'
            end as idt_sexo,
            cod_receita,
            min_idade,
            max_idade,
            per_desconto,
            idc_ativa = 'S' as idc_ativa,
            idc_suspensa = 'S' as idc_suspensa,
            idc_inscricao_fila = 'S' as idc_inscricao_fila,
            idc_fila_suspensa = 'S' as idc_fila_suspensa,
            case idt_status
               when 'F' then 'Fila'
               when 'L' then 'Livre'
               when 'A' then 'Autorizada'
            end as idt_status,
            idc_chamada_auto = 'S' as idc_chamada_auto,
            per_desconto_matr,
            dat_inclusao,
            idc_aceita_transf = 'S' as idc_aceita_transf,
            cod_curso_fila, 
            cod_turma_fila,
            idc_muda_situacao_fila = 'S' as idc_muda_situacao_fila,
            horario_turma.hora_inicio,
            horario_turma.hora_fim,
            nvl((select 1 from {linha_log.banco}:dias_let_curso as dias_let_curso where dias_let_curso.cod_curso = turma.cod_curso and dias_let_curso.cod_turma = turma.cod_turma and cod_dias_let = 'SEGUNDA'),0) as idc_seg,
            nvl((select 1 from {linha_log.banco}:dias_let_curso as dias_let_curso where dias_let_curso.cod_curso = turma.cod_curso and dias_let_curso.cod_turma = turma.cod_turma and cod_dias_let = 'TERCA'),0) as idc_ter,
            nvl((select 1 from {linha_log.banco}:dias_let_curso as dias_let_curso where dias_let_curso.cod_curso = turma.cod_curso and dias_let_curso.cod_turma = turma.cod_turma and cod_dias_let = 'QUARTA'),0) as idc_qua,
            nvl((select 1 from {linha_log.banco}:dias_let_curso as dias_let_curso where dias_let_curso.cod_curso = turma.cod_curso and dias_let_curso.cod_turma = turma.cod_turma and cod_dias_let = 'QUINTA'),0) as idc_qui,
            nvl((select 1 from {linha_log.banco}:dias_let_curso as dias_let_curso where dias_let_curso.cod_curso = turma.cod_curso and dias_let_curso.cod_turma = turma.cod_turma and cod_dias_let = 'SEXTA'),0) as idc_sex,
            nvl((select 1 from {linha_log.banco}:dias_let_curso as dias_let_curso where dias_let_curso.cod_curso = turma.cod_curso and dias_let_curso.cod_turma = turma.cod_turma and cod_dias_let = 'SABADO'),0) as idc_sab,
            nvl((select 1 from {linha_log.banco}:dias_let_curso as dias_let_curso where dias_let_curso.cod_curso = turma.cod_curso and dias_let_curso.cod_turma = turma.cod_turma and cod_dias_let = 'DOMINGO'),0) as idc_dom
        from {linha_log.banco}:turma as turma
        inner join {linha_log.banco}:horario_turma as horario_turma on
           horario_turma.cod_horario = turma.cod_horario
        where
            cod_curso = ? and
            cod_turma = ?
    """,(
        chave.cod_curso,
        chave.cod_turma,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None
    
    if not origem:
        raise Exception('Turma não existente')
    
    # Busca Ids no minascorp
    #
    cr_sql.execute(f"""
        select
            Turma.IdTurma, 
            Curso.IdCurso, 
            (select PkSql from PkDePara where Tabela = 'Nivel' and PkIfx = '{origem.cod_nivel}') as IdNivel, 
            Local.IdLocal, 
            Receita.IdReceita, 
            TurmaCompartilhada.IdTurma as IdTurmaCompartilhada
        from Clube
        left join Curso on
            Curso.IdClube = Clube.IdClube and
            Curso.CodigoCurso = '{origem.cod_curso}'
        left join Turma on
            Turma.IdCurso = Curso.IdCurso and
            Turma.CodigoTurma = '{origem.cod_turma}'
        left join Local on
            IdUnidade = (select PkSql from PkDePara where Tabela = 'Unidade' and PkIfx = '{cod_clube}|{origem.cod_unidade}') and
            nro_seq_local = {origem.nro_seq_local}
        left join Receita on
            Receita.IdClube = Clube.IdClube and
            Receita.CodigoReceita = {origem.cod_receita}
        left join Curso as CursoCompartilhado on
            CursoCompartilhado.IdClube = Clube.IdClube and
            CursoCompartilhado.CodigoCurso = '{origem.cod_curso_fila}'
        left join Turma as TurmaCompartilhada on
            TurmaCompartilhada.IdCurso = CursoCompartilhado.IdCurso and
            TurmaCompartilhada.CodigoTurma = '{origem.cod_turma_fila}'
        where
            Clube.IdClube = '{origem.cod_clube}'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    dados = Linha(*linha) if linha else None
        

    # Turma
    #
    cr_sql.execute("""
        update Turma set
            IdCurso = ?,
            CodigoTurma = ?,
            NomeTurma = ?,
            IdNivel = ?,
            IdLocal = ?,
            Competicao = ?,
            Vagas = ?,
            Sexo = ?,
            IdReceita = ?,
            IdadeMinima = ?,
            IdadeMaxima = ?,
            PercentualAcrescimo = ?,
            Ativa = ?,
            TurmaSuspensa = ?,
            FilaEspera = ?,
            FilaSuspensa = ?,
            Status = ?,
            ChamadaAutomatica = ?,
            PercentualDescontoMatricula = ?,
            DataCriacao = ?,
            AceitaTransferencia = ?,
            IdTurmaFilaEsperaCompartilhada = ?,
            MudaSituacaoFilaCompartilhada = ?,
            UltimaAlteracao = getdate()
        where
            IdTurma = ?
    """,(
            dados.IdCurso,
            origem.cod_turma, 
            origem.des_turma,
            dados.IdNivel,
            dados.IdLocal,
            origem.idc_competicao,
            origem.nro_vagas,
            origem.idt_sexo,
            dados.IdReceita,
            origem.min_idade,
            origem.max_idade,
            origem.per_desconto,
            origem.idc_ativa,
            origem.idc_suspensa,
            origem.idc_inscricao_fila,
            origem.idc_fila_suspensa,
            origem.idt_status,
            origem.idc_chamada_auto,
            origem.per_desconto_matr,
            origem.dat_inclusao,
            origem.idc_aceita_transf,
            dados.IdTurmaCompartilhada, 
            origem.idc_muda_situacao_fila,
            dados.IdTurma,
    ))

    if cr_sql.rowcount == 0:

        cr_sql.execute("""
            insert into Turma
            (
                IdCurso,
                CodigoTurma,
                NomeTurma,
                IdNivel,
                IdLocal,
                Competicao,
                Vagas,
                Sexo,
                IdReceita,
                IdadeMinima,
                IdadeMaxima,
                PercentualAcrescimo,
                Ativa,
                TurmaSuspensa,
                FilaEspera,
                FilaSuspensa,
                Status,
                ChamadaAutomatica,
                PercentualDescontoMatricula,
                DataCriacao,
                AceitaTransferencia,
                IdTurmaFilaEsperaCompartilhada,
                MudaSituacaoFilaCompartilhada
            ) values (
                ? /*IdCurso*/,
                ? /*CodigoTurma*/,
                ? /*NomeTurma*/,
                ? /*IdNivel*/,
                ? /*IdLocal*/,
                ? /*Competicao*/,
                ? /*Vagas*/,
                ? /*Sexo*/,
                ? /*IdReceita*/,
                ? /*IdadeMinima*/,
                ? /*IdadeMaxima*/,
                ? /*PercentualAcrescimo*/,
                ? /*Ativa*/,
                ? /*TurmaSuspensa*/,
                ? /*FilaEspera*/,
                ? /*FilaSuspensa*/,
                ? /*Status*/,
                ? /*ChamadaAutomatica*/,
                ? /*PercentualDescontoMatricula*/,
                ? /*DataCriacao*/,
                ? /*AceitaTransferencia*/,
                ? /*IdTurmaFilaEsperaCompartilhada*/,
                ? /*MudaSituacaoFilaCompartilhada*/
            )
        """,(
            dados.IdCurso,
            origem.cod_turma,
            origem.des_turma,
            dados.IdNivel,
            dados.IdLocal,
            origem.idc_competicao,
            origem.nro_vagas,
            origem.idt_sexo,
            origem.cod_receita,
            origem.min_idade,
            origem.max_idade,
            origem.per_desconto,
            origem.idc_ativa,
            origem.idc_suspensa,
            origem.idc_inscricao_fila,
            origem.idc_fila_suspensa,
            origem.idt_status,
            origem.idc_chamada_auto,
            origem.per_desconto_matr,
            origem.dat_inclusao,
            origem.idc_aceita_transf,
            dados.IdTurmaCompartilhada,
            origem.idc_muda_situacao_fila
        ))
        
        cr_sql.execute("""select ident_current('Turma')""")
        dados.IdTurma = cr_sql.fetchval()

    # Horario turma
    #
    cr_sql.execute("""
        update HorarioTurma set
            HoraInicio = ?,
            HoraFim = ?,
            Segunda = ?,
            Terca = ?,
            Quarta = ?,
            Quinta = ?,
            Sexta = ?,
            Sabado = ?,
            Domingo = ?,
            UltimaAlteracao = getdate()
        where
            IdTurma = ? 
    """,(
            origem.hora_inicio,
            origem.hora_fim,
            origem.idc_seg,
            origem.idc_ter,
            origem.idc_qua,
            origem.idc_qui,
            origem.idc_sex,
            origem.idc_sab,
            origem.idc_dom,
            dados.IdTurma,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute("""
            insert into HorarioTurma
            (
                IdTurma,
                HoraInicio,
                HoraFim,
                Segunda,
                Terca,
                Quarta,
                Quinta,
                Sexta,
                Sabado,
                Domingo
            ) values (
                ? /*IdTurma*/,
                ? /*HoraInicio*/,
                ? /*HoraFim*/,
                ? /*Segunda*/,
                ? /*Terca*/,
                ? /*Quarta*/,
                ? /*Quinta*/,
                ? /*Sexta*/,
                ? /*Sabado*/,
                ? /*Domingo*/
            )
        """,(
            dados.IdTurma,
            origem.hora_inicio,
            origem.hora_fim,
            origem.idc_seg,
            origem.idc_ter,
            origem.idc_qua,
            origem.idc_qui,
            origem.idc_sex,
            origem.idc_sab,
            origem.idc_dom,
        ))


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
            tabela = 'turma' and tentativas = 57
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
