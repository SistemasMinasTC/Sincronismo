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

    linha_log.pk = 'Cidadao|'+linha_log.pk

    Chave = recordtype('Chave', 'cod_origem, idf_cidadao')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            nom_cidadao,
            cpf_cidadao,
            case idt_sexo
                when 'M' then 'Masculino'
                when 'F' then 'Feminino'
            end as idt_sexo,
            dat_nascimento,
            nom_pai,
            nom_mae,
            nro_identidade,
            'RG' as cod_tipo_documento,
            dat_exp_identidade,
            org_exp_identidade,
            des_email,
            nro_celular,
            des_logradouro,
            nro_endereco,
            des_complemento,
            nom_bairro,
            nom_cidade,
            cod_uf,
            nro_cep,
            nom_pais
        from {linha_log.banco}:cidadao as cidadao
        where
            idf_cidadao = ?
    """,(
        chave.idf_cidadao,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Pessoa set
            Nome = ?,
            CPF = ?,
            Sexo = ?,
            DataNascimento = ?,
            NomePai = ?,
            NomeMae = ?,
            NumeroDocumento = ?,
            IdTipoDocumento = (select PkSql from PkDePara where Tabela = 'TipoDocumento' and PkIfx = ?),
            OrgaoEmissao = ?,
            DataExpedicaoDocumento = ?,
            Email = ?,
            Celular = ?,
            Logradouro = ?,
            NumeroEndereco = ?,
            ComplementoEndereco = ?,
            Bairro = ?,
            Cidade = ?,
            IdUF = (select PkSql from PkDePara where Tabela = 'UF' and PkIfx = ?),
            CEP = ?,
            Pais = ?,
            UltimaAlteracao = getdate()
        where
            IdPessoa = (select PkSql from PkDePara where Tabela = 'Pessoa' and PkIfx = ?)
    """,(
            origem.nom_cidadao,
            origem.cpf_cidadao,
            origem.idt_sexo,
            origem.dat_nascimento,
            origem.nom_pai,
            origem.nom_mae,
            origem.nro_identidade,
            origem.cod_tipo_documento,
            origem.org_exp_identidade,
            origem.dat_exp_identidade,
            origem.des_email,
            origem.nro_celular,
            origem.des_logradouro,
            origem.nro_endereco,
            origem.des_complemento,
            origem.nom_bairro,
            origem.nom_cidade,
            origem.cod_uf,
            origem.nro_cep,
            origem.nom_pais,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Pessoa
            (
                Nome,
                CPF,
                Sexo,
                DataNascimento,
                NomePai,
                NomeMae,
                NumeroDocumento,
                IdTipoDocumento,
                OrgaoEmissao,
                DataExpedicaoDocumento,
                Email,
                Celular,
                Logradouro,
                NumeroEndereco,
                ComplementoEndereco,
                Bairro,
                Cidade,
                IdUF,
                CEP,
                Pais
            ) values (
                ? /*Nome*/,
                ? /*CPF*/,
                ? /*Sexo*/,
                ? /*DataNascimento*/,
                ? /*NomePai*/,
                ? /*NomeMae*/,
                ? /*IdTipoDocumento*/,
                (select PkSql from PkDePara where Tabela = 'TipoDocumento' and PkIfx = ?) /*NumeroDocumento*/,
                ? /*OrgaoEmissao*/,
                ? /*DataExpedicaoDocumento*/,
                ? /*Email*/,
                ? /*Celular*/,
                ? /*Logradouro*/,
                ? /*NumeroEndereco*/,
                ? /*ComplementoEndereco*/,
                ? /*Bairro*/,
                ? /*Cidade*/,
                (select PkSql from PkDePara where Tabela = 'UF' and PkIfx = ?) /*IdUF*/,
                ? /*CEP*/,
                ? /*Pais*/
            )
        """,(
            origem.nom_cidadao,
            origem.cpf_cidadao,
            origem.idt_sexo,
            origem.dat_nascimento,
            origem.nom_pai,
            origem.nom_mae,
            origem.nro_identidade,
            origem.cod_tipo_documento,
            origem.org_exp_identidade,
            origem.dat_exp_identidade,
            origem.des_email,
            origem.nro_celular,
            origem.des_logradouro,
            origem.nro_endereco,
            origem.des_complemento,
            origem.nom_bairro,
            origem.nom_cidade,
            origem.cod_uf,
            origem.nro_cep,
            origem.nom_pais
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

    ifx = conecta_informix('minasdba')
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
            tabela = 'cidadao'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
