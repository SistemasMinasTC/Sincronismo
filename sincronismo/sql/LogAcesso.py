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

    cr_sql.execute(f"""
        select
            (select PkIfx from PkDePara where Tabela = 'Portaria' and PkSql = LogAcesso.IdPortaria) as PkPortaria,
            (select PkIfx from PkDePara where Tabela = 'Local' and PkSql = Portaria.IdLocal) as PkLocal,
            NumeroSequenciaDispositivo,
            cast(DataMovimento as date) DataMovimento,
            cast(DataMovimento as time(3)) HoraMovimento,
            (select PkIfx from PkDePara with (nolock) where Tabela = 'Pessoa' and PkSql = LogAcesso.IdPessoa) as PkPessoa,
            case
                when Entrada = 1 then 'E'
                else 'S'
            end as Entrada,
            Matricula,
            (select PkIfx from PkDePara  with (nolock) where Tabela = 'MotivoLiberacao' and PkSql = IdMotivoLiberacao) as cod_mot_liberacao,
            isnull(dbo.IdadeNaData(Pessoa.DataNascimento,DataMovimento),99) as Idade,
            case Pessoa.Sexo
                when 'Masculino' then 'M'
                when 'Feminino' then 'F'
                else 'A'
            end as idt_sexo,
            isnull(TipoCota,'XX') as TipoCota,
            Cota.NumeroCota,
            case
                when MetodoDeAcesso ='CARTÃO MAGNÉTICO' then 'L'
                when MetodoDeAcesso ='CARTÃO RFID' then 'R'
                when MetodoDeAcesso ='MANUAL' then 'T'
                when MetodoDeAcesso ='DIGITAL' then 'S'
                when MetodoDeAcesso ='QR-CODE' then 'Q'
                else 'L'
            end as idt_origem,
            LogAcesso.IdPortaria,
            (select PkIfx from PkDePara  with (nolock) where Tabela = 'ModalidadeEsportiva' and PkSql = IdModalidadeEsportiva) as PkModalidadeEsportiva,
            PkDePara.PkIfx
        from LogAcesso with (nolock)
        inner join Portaria with (nolock) on Portaria.IdPortaria = LogAcesso.IdPortaria
        left join Pessoa with (nolock) on Pessoa.IdPessoa = LogAcesso.IdPessoa
        left join Associado with (nolock) on
            Associado.IdPessoa = LogAcesso.IdPessoa and
            Associado.Prioritario = 1
        left join Cota with (nolock) on Cota.IdCota = Associado.IdCota
        left join Turma with (nolock) on Turma.IdTurma = LogAcesso.IdTurma
        left join Curso with (nolock) on Curso.IdCurso = Turma.IdCurso
        left join PkDePara on
            Tabela = 'LogAcesso' and
            PkDePara.PkSql = IdLogAcesso
        where
            IdLogAcesso = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        select count(*) as quantidadePortaria
        from PortariaCurso with(nolock)
        where
           IdPortaria = ?
    """,(
        origem.IdPortaria
    ))

    academia = cr_sql.fetchone()[0]
    cr_sql.close()

    if not origem or not origem.PkPortaria:
        return

    cod_clube, cod_unidade,cod_portaria = origem.PkPortaria.split('|')
    nro_seq_local = origem.PkLocal.split('|')[2] if origem.PkLocal else None

    if origem.PkPessoa:
        if 'Associado|' in origem.PkPessoa:
            idt_pessoa = 1
            cod_pessoa = origem.PkPessoa.split('|')[1]
        elif '|Acompanhante|' in origem.PkPessoa:
            idt_pessoa = 3
            cod_pessoa = origem.PkPessoa.split('|')[2]
    else:
        idt_pessoa = cod_pessoa = None

    banco = 'minas' if cod_clube == 'MTC' else 'nautico'

    if not academia:
        if not origem.PkIfx:
            origem.PkIfx = '|'.join((cod_unidade,cod_portaria,str(origem.NumeroSequenciaDispositivo),str(origem.DataMovimento),str(origem.HoraMovimento)[:-3]))

        Chave = recordtype('Chave', 'cod_unidade,cod_portaria,nro_seq_disp,dat_movto,hor_movto')
        chave = Chave(*origem.PkIfx.split('|'))

        if linha_log.operacao == 'del':
            cr_ifx.execute(f"""
                delete from {banco}:log_movto_portaria
                where
                    cod_unidade = ? and
                    cod_portaria = ? and
                    nro_seq_disp = ? and
                    dat_movto = ? and
                    hor_movto = ?
            """, (
                chave.cod_unidade,
                chave.cod_portaria,
                chave.nro_seq_disp,
                chave.dat_movto,
                chave.hor_movto,
            ))

            return

        cr_ifx.execute(f"""
            update {banco}:log_movto_portaria set
                idt_pessoa = ?,
                cod_pessoa = ?,
                idt_sentido = ?,
                mla_funcionario = ?,
                cod_mot_liberacao = ?,
                idade = ?,
                idt_sexo = ?,
                cod_tipo_associado = ?,
                idt_origem = ?,
                cod_parentesco = (
                    select cod_parentesco from {banco}:cota_associado as cota_associado
                    where
                        cod_tipo_associado = ? and
                        cod_cota = ? and
                        cod_associado = ?
                )
            where
                cod_unidade = ? and
                cod_portaria = ? and
                nro_seq_disp = ? and
                dat_movto = ? and
                hor_movto = ?
        """,(
                idt_pessoa,
                cod_pessoa,
                origem.Entrada,
                origem.Matricula,
                origem.cod_mot_liberacao,
                origem.Idade,
                origem.idt_sexo,
                origem.TipoCota,
                origem.idt_origem,
                origem.TipoCota,
                origem.NumeroCota,
                cod_pessoa,
                chave.cod_unidade,
                chave.cod_portaria,
                chave.nro_seq_disp,
                chave.dat_movto,
                chave.hor_movto,
        ))

        if cr_ifx.rowcount == 0:
            cr_ifx.execute(f"""
                insert into {banco}:log_movto_portaria
                (
                    cod_unidade,
                    cod_portaria,
                    nro_seq_disp,
                    dat_movto,
                    hor_movto,
                    idt_pessoa,
                    cod_pessoa,
                    idt_sentido,
                    mla_funcionario,
                    cod_mot_liberacao,
                    idade,
                    idt_sexo,
                    cod_tipo_associado,
                    idt_origem,
                    cod_parentesco,
                    idc_aluno,
                    idc_militante,
                    idt_status_movto,
                    idc_cart_porteiro
                ) values (
                    ? {{cod_unidade}},
                    ? {{cod_portaria}},
                    ? {{nro_seq_disp}},
                    ? {{dat_movto}},
                    ? {{hor_movto}},
                    ? {{idt_pessoa}},
                    ? {{cod_pessoa}},
                    ? {{idt_sentido}},
                    ? {{mla_funcionario}},
                    ? {{cod_mot_liberacao}},
                    ? {{idade}},
                    ? {{idt_sexo}},
                    ? {{cod_tipo_associado}},
                    ? {{idt_origem}},
                    (
                        select cod_parentesco from {banco}:cota_associado as cota_associado
                        where
                            cod_tipo_associado = ? and
                            cod_cota = ? and
                            cod_associado = ?
                    ) {{cod_parentesco}},
                    nvl((select distinct 'S' from {banco}:aluno as aluno where cod_associado = ?),'N') {{idc_aluno}},
                    nvl
                    ((
                        select 'S'
                        from {banco}:_cota_ as _cota_
                        where
                        _cota_.cod_tipo_associado = 'CC' and
                        _cota_.cod_associado_tit = ? and
                        _cota_.cod_tipo_cota = 'MILIB' and
                        nvl(_cota_.dat_prev_encerra,today) <= today
                    ),'N') {{idc_militante}},
                    'S' {{idt_status_movto}},
                    'N' {{idc_cart_porteiro}}
                )
            """,(
                chave.cod_unidade,
                chave.cod_portaria,
                chave.nro_seq_disp,
                chave.dat_movto,
                chave.hor_movto,
                idt_pessoa,
                cod_pessoa,
                origem.Entrada,
                origem.Matricula,
                origem.cod_mot_liberacao,
                origem.Idade,
                origem.idt_sexo,
                origem.TipoCota,
                origem.idt_origem,
                origem.TipoCota,
                origem.NumeroCota,
                cod_pessoa,
                cod_pessoa,
                cod_pessoa,
            ))

            #cr_sql = conn_sql.cursor()
            #cr_sql.execute("insert into PkDePara values ('LogAcesso',?,?)",(linha_log.pk, origem.PkIfx))
            #cr_sql.close()
    else: # Academmia
        if not origem.PkIfx:
            origem.PkIfx = '|'.join((cod_unidade,nro_seq_local,str(origem.NumeroSequenciaDispositivo),f'{origem.DataMovimento} {str(origem.HoraMovimento)[:-3]}'))

        Chave = recordtype('Chave', 'cod_unidade,nro_seq_local,nro_seq_disp,dat_hor_movto')
        chave = Chave(*origem.PkIfx.split('|'))

        cr_ifx.execute(f"""
            update log_movto_local
            set
            (
                cod_associado,
                idt_sentido,
                mla_funcionario,
                cod_mot_liberacao,
                cod_ocorrencia,
                cod_mod_curso,
                idt_origem
            ) = (
                ? /*cod_associado*/,
                ? /*idt_sentido*/,
                ? /*mla_funcionario*/,
                ? /*cod_mot_liberacao*/,
                ? /*cod_ocorrencia*/,
                ? /*cod_mod_curso*/,
                ? /*idt_origem*/
            )
            where
                cod_unidade = ? and
                nro_seq_local = ? and
                nro_seq_disp = ? and
                dat_hor_movto = ?
         """,(
                cod_pessoa,
                origem.Entrada,
                origem.Matricula,
                99,
                origem.cod_mot_liberacao,
                origem.PkModalidadeEsportiva.split('|')[1] if origem.PkModalidadeEsportiva else None,
                origem.idt_origem,

                chave.cod_unidade,
                nro_seq_local,
                chave.nro_seq_disp,
                chave.dat_hor_movto,
        ))

        if cr_ifx.rowcount == 0:
            cr_ifx.execute(f"""
                insert into log_movto_local
                (
                    cod_unidade,
                    nro_seq_local,
                    nro_seq_disp,
                    dat_hor_movto,
                    cod_associado,
                    idt_sentido,
                    mla_funcionario,
                    cod_mot_liberacao,
                    cod_ocorrencia,
                    cod_mod_curso,
                    idt_origem
                ) values (
                    ? /*cod_unidade*/,
                    ? /*nro_seq_local*/,
                    ? /*nro_seq_disp*/,
                    ? /*dat_hor_movto*/,
                    ? /*cod_associado*/,
                    ? /*idt_sentido*/,
                    ? /*mla_funcionario*/,
                    ? /*cod_mot_liberacao*/,
                    ? /*cod_ocorrencia*/,
                    ? /*cod_mod_curso*/,
                    ? /*idt_origem*/
                )
                """,(
                    chave.cod_unidade,
                    nro_seq_local,
                    chave.nro_seq_disp,
                    chave.dat_hor_movto,
                    cod_pessoa,
                    origem.Entrada,
                    origem.Matricula,
                    99,
                    origem.cod_mot_liberacao,
                    origem.PkModalidadeEsportiva.split('|')[1] if origem.PkModalidadeEsportiva else None,
                    origem.idt_origem,
                ))

            cr_sql = conn_sql.cursor()
            cr_sql.execute("insert into PkDePara values ('LogAcesso',?,?)",(linha_log.pk, origem.PkIfx))
            cr_sql.close()

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
            pk
        from mc_log
        where
            atualizacao is null and
            tabela = 'LogAcesso'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)

