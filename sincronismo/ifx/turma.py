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

    Chave = recordtype('Chave', 'cod_clube, cod_curso, cod_turma')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
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
            '{cod_clube}|' || cod_curso as cod_curso,
            cod_turma,
            trim(des_turma) as des_turma,
            cod_nivel,
            '{cod_clube}|' || cod_unidade || '|' || nro_seq_local as nro_seq_local,
            idc_competicao = 'S' as idc_competicao,
            nro_vagas,
            case idt_sexo
                when 'M' then 'Masculino'
                when 'F' then 'Feminino'
                when 'A' then 'Ambos'
            end as idt_sexo,
            '{cod_clube}|' || cod_receita as cod_receita,
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

    # Turma
    #
    cr_sql.execute(f"""
        update Turma set
            IdCurso = (select PkSql from PkDePara where Tabela = 'Curso' and PkIfx = ?),
            CodigoTurma = ?,
            NomeTurma = ?,
            IdNivel = (select PkSql from PkDePara where Tabela = 'Nivel' and PkIfx = ?),
            IdLocal = (select PkSql from PkDePara where Tabela = 'Local' and PkIfx = ?),
            Competicao = ?,
            Vagas = ?,
            Sexo = ?,
            IdReceita = (select PkSql from PkDePara where Tabela = 'Receita' and PkIfx = ?),
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
            IdTurmaFilaEsperaCompartilhada = (select PkSql from PkDePara where Tabela = 'Turma' and PkIfx = ?),
            MudaSituacaoFilaCompartilhada = ?,
            UltimaAlteracao = getdate()
        where
            IdTurma = (select PkSql from PkDePara where Tabela = 'Turma' and PkIfx = ?)
    """,(
            origem.cod_curso,
            origem.cod_turma,
            origem.des_turma,
            origem.cod_nivel,
            origem.nro_seq_local,
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
            origem.cod_turma_fila,
            origem.idc_muda_situacao_fila,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
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
            ) output inserted.IdTurma  values (
                (select PkSql from PkDePara where Tabela = 'Curso' and PkIfx = ?) /*IdCurso*/,
                ? /*CodigoTurma*/,
                ? /*NomeTurma*/,
                (select PkSql from PkDePara where Tabela = 'Nivel' and PkIfx = ?) /*IdNivel*/,
                (select PkSql from PkDePara where Tabela = 'Local' and PkIfx = ?) /*IdLocal*/,
                ? /*Competicao*/,
                ? /*Vagas*/,
                ? /*Sexo*/,
                (select PkSql from PkDePara where Tabela = 'Receita' and PkIfx = ?) /*IdReceita*/,
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
                (select PkSql from PkDePara where Tabela = 'Turma' and PkIfx = ?) /*IdTurmaFilaEsperaCompartilhada*/,
                ? /*MudaSituacaoFilaCompartilhada*/
            )
        """,(
            origem.cod_curso,
            origem.cod_turma,
            origem.des_turma,
            origem.cod_nivel,
            origem.nro_seq_local,
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
            origem.cod_turma_fila,
            origem.idc_muda_situacao_fila
        ))

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Turma',?,?)",(pkSql, linha_log.pk,))
        cr_sql.execute("commit transaction")

    # Horario turma
    #
    cr_sql.execute("""
        update HorarioTurma set
            IdTurma = (select PkSql from PkDePara where Tabela = 'Turma' and PkIfx = ?),
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
            IdHorarioTurma = (select PkSql from PkDePara where Tabela = 'HorarioTurma' and PkIfx = ?)
    """,(
            linha_log.pk,
            origem.hora_inicio,
            origem.hora_fim,
            origem.idc_seg,
            origem.idc_ter,
            origem.idc_qua,
            origem.idc_qui,
            origem.idc_sex,
            origem.idc_sab,
            origem.idc_dom,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
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
            ) output inserted.IdHorarioTurma values (
                (select PkSql from PkDePara where Tabela = 'Turma' and PkIfx = ?) /*IdTurma*/,
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
            linha_log.pk,
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

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('HorarioTurma',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'turma'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        #try:
        convert(ifx, sql, linha)
        #except Exception as erro:
        #    print(erro)
