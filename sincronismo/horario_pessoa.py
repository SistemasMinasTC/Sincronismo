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

    Chave = recordtype('Chave', 'cod_clube, idf_horario_pessoa')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from HorarioAcompanhante
            where
                IdHorarioAcompanhante = (select PkSql from PkDePara where Tabela = 'HorarioAcompanhante' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            cod_pessoa,
            cod_turma,
            hor_ini_perm,
            hor_fim_perm,
            dat_ini_perm,
            dat_fim_perm,
            des_dias_semana matches '*SEG*' as idt_segunda,
            des_dias_semana matches '*TER*' as idt_terca,
            des_dias_semana matches '*QUA*' as idt_quarta,
            des_dias_semana matches '*QUI*' as idt_quinta,
            des_dias_semana matches '*SEX*' as idt_sexta,
            des_dias_semana matches '*SAB*' as idt_sabado,
            des_dias_semana matches '*DOM*' as idt_domingo
        from
        (
            select
                '{cod_clube}|' || cod_pessoa as cod_pessoa,
                '{cod_clube}|' || cod_curso || '|' || cod_turma as cod_turma,
                to_char(hor_ini_perm,'%H:%M:%S') as hor_ini_perm,
                to_char(hor_fim_perm,'%H:%M:%S') as hor_fim_perm,
                dat_ini_perm,
                dat_fim_perm,
                replace(
                    replace(
                        replace(
                            replace(
                            replace(
                                des_dias_semana,'SEG A QUI','SEG,TER,QUA,QUI'
                            ),'SEG A SEX','SEG,TER,QUA,QUI,SEX'
                            ),'SEG A SAB','SEG,TER,QUA,QUI,SEX,SAB'
                        ),'TER A SEX','TER,QUA,QUI,SEX'
                    ),' ',''
                ) as des_dias_semana
            from {linha_log.banco}:horario_pessoa as horario_pessoa
            where
                idf_horario_pessoa = ?
        )
    """,(
        chave.idf_horario_pessoa,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update HorarioAcompanhante set
            IdAcompanhante = (select PkSql from PkDePara where Tabela = 'Acompanhante' and PkIfx = ?),
            IdTurma = (select PkSql from PkDePara where Tabela = 'Turma' and PkIfx = ?),
            HoraInicio = ?,
            HoraFim = ?,
            DataInicio = ?,
            DataFim = ?,
            Segunda = ?,
            Terca = ?,
            Quarta = ?,
            Quinta = ?,
            Sexta = ?,
            Sabado = ?,
            Domingo = ?,
            UltimaAlteracao = getdate()
        where
            IdHorarioAcompanhante = (select PkSql from PkDePara where Tabela = 'HorarioAcompanhante' and PkIfx = ?)
    """,(
            origem.cod_pessoa,
            origem.cod_turma,
            origem.hor_ini_perm,
            origem.hor_fim_perm,
            origem.dat_ini_perm,
            origem.dat_fim_perm,
            origem.idt_segunda,
            origem.idt_terca,
            origem.idt_quarta,
            origem.idt_quinta,
            origem.idt_sexta,
            origem.idt_sabado,
            origem.idt_domingo,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into HorarioAcompanhante
            (
                IdAcompanhante,
                IdTurma,
                HoraInicio,
                HoraFim,
                DataInicio,
                DataFim,
                Segunda,
                Terca,
                Quarta,
                Quinta,
                Sexta,
                Sabado,
                Domingo
            ) values (
                (select PkSql from PkDePara where Tabela = 'Acompanhante' and PkIfx = ?) /*IdAcompanhante*/,
                (select PkSql from PkDePara where Tabela = 'Turma' and PkIfx = ?) /*IdTurma*/,
                ? /*HoraInicio*/,
                ? /*HoraFim*/,
                ? /*DataInicio*/,
                ? /*DataFim*/,
                ? /*Segunda*/,
                ? /*Terca*/,
                ? /*Quarta*/,
                ? /*Quinta*/,
                ? /*Sexta*/,
                ? /*Sabado*/,
                ? /*Domingo*/
            )
        """,(
            origem.cod_pessoa,
            origem.cod_turma,
            origem.hor_ini_perm,
            origem.hor_fim_perm,
            origem.dat_ini_perm,
            origem.dat_fim_perm,
            origem.idt_segunda,
            origem.idt_terca,
            origem.idt_quarta,
            origem.idt_quinta,
            origem.idt_sexta,
            origem.idt_sabado,
            origem.idt_domingo,
        ))

        cr_sql.execute("""select ident_current('HorarioAcompanhante')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('HorarioAcompanhante',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'horario_pessoa'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
