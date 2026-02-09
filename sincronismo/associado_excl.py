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

    linha_log.pk = 'Associado|'+linha_log.pk

    Chave = recordtype('Chave', 'nom_origem, cod_associado')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            nom_associado,
            cpf_associado,
            case idt_estado_civil
                when 'S' then 'Solteiro'
                when 'C' then 'Casado'
                when 'V' then 'Viúvo'
                when 'M' then 'Marital'
                when 'O' then 'Outro'
            end as idt_estado_civil,
            case idt_sexo
                when 'M' then 'Masculino'
                when 'F' then 'Feminino'
            end as idt_sexo,
            dat_nascimento,
            (select des_cidade from cidade where cidade.cod_uf = associado_excl.cod_uf_nascimento and cidade.cod_cidade = associado_excl.cod_cidade_nasc) as nom_cidade_natal,
            (select uf.cod_uf from uf where uf.cod_uf = cod_uf_nascimento) as cod_uf_nascimento, --null se não estiver na tabela uf
            cod_nacionalidade,
            nom_pai,
            nom_mae,
            cod_profissao,
            nom_empresa_trab,
            nom_cargo_exerce,
            tip_identidade,
            nro_identidade,
            org_emissor,
            (select uf.cod_uf from uf where uf.cod_uf = uf_emissor) as uf_emissor, --null se não estiver na tabela uf
            dat_exp_identidade,
            des_email,
            dat_falecimento
        from {linha_log.banco}:associado_excl as associado_excl
        where
            cod_associado = ?
    """,(
        chave.cod_associado,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    if not origem:
        return

    cr_sql.execute("""
        update Pessoa set
            Nome = ?,
            CPF = ?,
            EstadoCivil = ?,
            Sexo = ?,
            DataNascimento = ?,
            CidadeNatal = ?,
            IdUFNatal = ?,
            IdNacionalidade = (select PkSql from PkDePara where Tabela = 'Nacionalidade' and PkIfx = ?),
            NomePai = ?,
            NomeMae = ?,
            IdProfissao = (select PkSql from PkDePara where Tabela = 'Profissao' and PkIfx = ?),
            NomeEmpresaTrabalha = ?,
            NomeCargoExerce = ?,
            IdTipoDocumento = (select PkSql from PkDePara where Tabela = 'TipoDocumento' and PkIfx = ?),
            NumeroDocumento = ?,
            OrgaoEmissao = ?,
            IdUFEmissor = ?,
            DataExpedicaoDocumento = ?,
            Email = ?,
            DataFalecimento = ?,
            UltimaAlteracao = getdate()
        where
            IdPessoa = (select PkSql from PkDePara where Tabela = 'Pessoa' and PkIfx = ?)
    """,(
            origem.nom_associado,
            origem.cpf_associado,
            origem.idt_estado_civil,
            origem.idt_sexo,
            origem.dat_nascimento,
            origem.nom_cidade_natal,
            origem.cod_uf_nascimento,
            origem.cod_nacionalidade,
            origem.nom_pai,
            origem.nom_mae,
            origem.cod_profissao,
            origem.nom_empresa_trab,
            origem.nom_cargo_exerce,
            origem.tip_identidade,
            origem.nro_identidade,
            origem.org_emissor,
            origem.uf_emissor,
            origem.dat_exp_identidade,
            origem.des_email,
            origem.dat_falecimento,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Pessoa
            (
                Nome,
                CPF,
                EstadoCivil,
                Sexo,
                DataNascimento,
                CidadeNatal,
                IdUFNatal,
                IdNacionalidade,
                NomePai,
                NomeMae,
                IdProfissao,
                NomeEmpresaTrabalha,
                NomeCargoExerce,
                IdTipoDocumento,
                NumeroDocumento,
                OrgaoEmissao,
                IdUFEmissor,
                DataExpedicaoDocumento,
                Email,
                DataFalecimento
            ) values (
                ? /*Nome*/,
                ? /*CPF*/,
                ? /*EstadoCivil*/,
                ? /*Sexo*/,
                ? /*DataNascimento*/,
                ? /*CidadeNatal*/,
                ? /*IdUFNatal*/,
                (select PkSql from PkDePara where Tabela = 'Nacionalidade' and PkIfx = ?) /*IdNacionalidade*/,
                ? /*NomePai*/,
                ? /*NomeMae*/,
                (select PkSql from PkDePara where Tabela = 'Profissao' and PkIfx = ?) /*IdProfissao*/,
                ? /*NomeEmpresaTrabalha*/,
                ? /*NomeCargoExerce*/,
                (select PkSql from PkDePara where Tabela = 'TipoDocumento' and PkIfx = ?) /*IdTipoDocumento*/,
                ? /*NumeroDocumento*/,
                ? /*OrgaoEmissao*/,
                ? /*IdUFEmissor*/,
                ? /*DataExpedicaoDocumento*/,
                ? /*Email*/,
                ? /*DataFalecimento*/
            )
        """,(
            origem.nom_associado,
            origem.cpf_associado,
            origem.idt_estado_civil,
            origem.idt_sexo,
            origem.dat_nascimento,
            origem.nom_cidade_natal,
            origem.cod_uf_nascimento,
            origem.cod_nacionalidade,
            origem.nom_pai,
            origem.nom_mae,
            origem.cod_profissao,
            origem.nom_empresa_trab,
            origem.nom_cargo_exerce,
            origem.tip_identidade,
            origem.nro_identidade,
            origem.org_emissor,
            origem.uf_emissor,
            origem.dat_exp_identidade,
            origem.des_email,
            origem.dat_falecimento
        ))

        cr_sql.execute("""select ident_current('Pessoa')""")

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Pessoa',?,?)",(pkSql, linha_log.pk,))
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
            tabela,
            operacao,
            pk
        from mc_log
        where
            tabela = 'associado_excl'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
