#!/usr/bin/python
#

from recordtype import recordtype
import os

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

   cr_ifx.execute(f"""
      insert into mc_log
      (
         data_hora,
         banco,
         tabela,
         operacao,
         pk
      )
      select
         current,
         '{linha_log.banco}',
         'cota_associado',
         'upd',
         cod_associado || '|' || cod_tipo_associado || '|' || cod_cota
      from {linha_log.banco}:cota_associado
      where
         cod_associado = {chave.cod_associado}
    """)

   if linha_log.operacao == 'del':
      cr_sql.close()
      return

   cr_ifx.execute(f"""
      select
         endereco_correspondencia.nom_bairro,
         endereco_correspondencia.nro_cep,
         cpf_associado,
         nro_celular[1,20] as nro_celular,
         endereco_correspondencia.nom_cidade,
         endereco_correspondencia.des_complemento,
         dat_exp_identidade,
         dat_falecimento,
         dat_nascimento,
         des_email,
         cnpj_associado,
         (select des_cidade from {linha_log.banco}:cidade as cidade where cidade.cod_uf = associado.cod_uf_nascimento and cidade.cod_cidade = associado.cod_cidade_nasc) as nom_cidade_natal,
         case idt_estado_civil
            when 'S' then 'Solteiro'
            when 'C' then 'Casado'
            when 'V' then 'Viúvo'
            when 'M' then 'Marital'
            when 'O' then 'Outro'
         end as idt_estado_civil,
         cod_nacionalidade,
         cod_associado_responsavel,
         cod_profissao,
         tip_identidade,
         (select uf.cod_uf from uf where uf.cod_uf = endereco_correspondencia.cod_uf) as cod_uf, --para retorna null em uf não cadastrada
         (select uf.cod_uf from uf where uf.cod_uf = associado.uf_emissor) as uf_emissor,
         (select uf.cod_uf from uf where uf.cod_uf = associado.cod_uf_nascimento) as cod_uf_nascimento,
         idt_optin_whatsapp = 'S' as idt_recebe_whatsapp,
         endereco_correspondencia.des_logradouro,
         nom_associado,
         nom_cargo_exerce,
         nom_empresa_trab,
         nom_mae,
         nom_pai,
         nro_identidade,
         endereco_correspondencia.nro_endereco,
         org_emissor,
         endereco_correspondencia.nom_pais,
         idc_recebe_email = 'S' as idc_recebe_email,
         idc_recebe_revista = 'S' as idc_recebe_revista,
         idc_recebe_sms = 'S' as idc_recebe_sms,
         case idt_sexo
            when 'M' then 'Masculino'
            when 'F' then 'Feminino'
         end as idt_sexo,
         nro_telefone[1,20] as nro_telefone,
         nom_tipo_sanguineo,
         idc_uso_imagem = 'S' as idc_uso_imagem,
         idc_voluntariado = 'S' as idc_voluntariado,
         idf_whatsapp
      from {linha_log.banco}:associado as associado
      left join {linha_log.banco}:endereco_correspondencia as endereco_correspondencia on endereco_correspondencia.cod_associado = associado.cod_associado
      where
         associado.cod_associado = ?
   """,(
      chave.cod_associado,
   ))

   Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
   linha = cr_ifx.fetchone()
   origem = Linha(*linha) if linha else None

   if not origem:
        cr_sql.close()
        return

   cr_sql.execute("""
      update Pessoa set
         Bairro = ?,
         CEP = ?,
         CPF = ?,
         Celular = ?,
         Cidade = ?,
         ComplementoEndereco = ?,
         DataExpedicaoDocumento = ?,
         DataNascimento = ?,
         DataFalecimento = ?,
         Email = ?,
         CNPJ = ?,
         CidadeNatal = ?,
         EstadoCivil = ?,
         IdNacionalidade = (select PkSql from PkDePara where Tabela = 'Nacionalidade' and PkIfx = ?),
         IdProfissao = (select PkSql from PkDePara where Tabela = 'Profissao' and PkIfx = ?),
         IdTipoDocumento = (select PkSql from PkDePara where Tabela = 'TipoDocumento' and PkIfx = ?),
         IdUF = ?,
         IdUFEmissor = ?,
         IdUFNatal = ?,
         RecebeWhatsapp = ?,
         Logradouro = ?,
         Nome = ?,
         NomeCargoExerce = ?,
         NomeEmpresaTrabalha = ?,
         NomeMae = ?,
         NomePai = ?,
         NumeroDocumento = ?,
         NumeroEndereco = ?,
         OrgaoEmissao = ?,
         Pais = ?,
         RecebeEmail = ?,
         RecebeRevista = ?,
         RecebeSms = ?,
         Sexo = ?,
         Telefone = ?,
         TipoSanguineo = ?,
         UsoImagem = ?,
         Voluntario = ?,
         Whatsapp = ?,
         UltimaAlteracao = getdate()
      where
         IdPessoa = (select PkSql from PkDePara where Tabela = 'Pessoa' and PkIfx = ?)
   """,(
         origem.nom_bairro,
         origem.nro_cep,
         origem.cpf_associado,
         origem.nro_celular,
         origem.nom_cidade,
         origem.des_complemento,
         origem.dat_exp_identidade,
         origem.dat_nascimento,
         origem.dat_falecimento,
         origem.des_email,
         origem.cnpj_associado,
         origem.nom_cidade_natal,
         origem.idt_estado_civil,
         origem.cod_nacionalidade,
         origem.cod_profissao,
         origem.tip_identidade,
         origem.cod_uf,
         origem.uf_emissor,
         origem.cod_uf_nascimento,
         origem.idt_recebe_whatsapp,
         origem.des_logradouro,
         origem.nom_associado,
         origem.nom_cargo_exerce,
         origem.nom_empresa_trab,
         origem.nom_mae,
         origem.nom_pai,
         origem.nro_identidade,
         origem.nro_endereco,
         origem.org_emissor,
         origem.nom_pais,
         origem.idc_recebe_email,
         origem.idc_recebe_revista,
         origem.idc_recebe_sms,
         origem.idt_sexo,
         origem.nro_telefone,
         origem.nom_tipo_sanguineo,
         origem.idc_uso_imagem,
         origem.idc_voluntariado,
         origem.idf_whatsapp,
         linha_log.pk,
   ))

   if cr_sql.rowcount == 0:
      cr_sql.execute('begin transaction')

      cr_sql.execute(f"""
         insert into Pessoa
         (
            Bairro,
            CEP,
            CPF,
            Celular,
            Cidade,
            ComplementoEndereco,
            DataExpedicaoDocumento,
            DataNascimento,
            DataFalecimento,
            Email,
            CNPJ,
            CidadeNatal,
            EstadoCivil,
            IdNacionalidade,
            IdProfissao,
            IdTipoDocumento,
            IdUF,
            IdUFEmissor,
            IdUFNatal,
            RecebeWhatsapp,
            Logradouro,
            Nome,
            NomeCargoExerce,
            NomeEmpresaTrabalha,
            NomeMae,
            NomePai,
            NumeroDocumento,
            NumeroEndereco,
            OrgaoEmissao,
            Pais,
            RecebeEmail,
            RecebeRevista,
            RecebeSms,
            Sexo,
            Telefone,
            TipoSanguineo,
            UsoImagem,
            Voluntario,
            Whatsapp
         ) values (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            (select PkSql from PkDePara where Tabela = 'Nacionalidade' and PkIfx = ?),
            (select PkSql from PkDePara where Tabela = 'Profissao' and PkIfx = ?),
            (select PkSql from PkDePara where Tabela = 'TipoDocumento' and PkIfx = ?),
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
         )
      """,(
         origem.nom_bairro,
         origem.nro_cep,
         origem.cpf_associado,
         origem.nro_celular,
         origem.nom_cidade,
         origem.des_complemento,
         origem.dat_exp_identidade,
         origem.dat_nascimento,
         origem.dat_falecimento,
         origem.des_email,
         origem.cnpj_associado,
         origem.nom_cidade,
         origem.idt_estado_civil,
         origem.cod_nacionalidade,
         origem.cod_profissao,
         origem.tip_identidade,
         origem.cod_uf,
         origem.cod_uf,
         origem.cod_uf_nascimento,
         origem.idt_recebe_whatsapp,
         origem.des_logradouro,
         origem.nom_associado,
         origem.nom_cargo_exerce,
         origem.nom_empresa_trab,
         origem.nom_mae,
         origem.nom_pai,
         origem.nro_identidade,
         origem.nro_endereco,
         origem.org_emissor,
         origem.nom_pais,
         origem.idc_recebe_email,
         origem.idc_recebe_revista,
         origem.idc_recebe_sms,
         origem.idt_sexo,
         origem.nro_telefone,
         origem.nom_tipo_sanguineo,
         origem.idc_uso_imagem,
         origem.idc_voluntariado,
         origem.idf_whatsapp
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

    ifx = conecta_informix('minas')
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
            tabela = 'associado'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
