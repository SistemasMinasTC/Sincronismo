#!/usr/bin/python
#

from recordtype import recordtype

# Função de-para do cod_parentesco
"""
create function if not exists cod_vinculo(cod_parentesco varchar(50)) returning varchar(50);
   define cod_vinculo varchar(50);
   case cod_parentesco
      when 'ASCE5' then let cod_vinculo = 'ACEN COM DESCONTO';
      when 'ASCE4' then let cod_vinculo = 'ACENDENTE COM DESCONTO';
      when 'ASCE8' then let cod_vinculo = 'ACENDENTE COM DESCONTO';
      when 'ASC10' then let cod_vinculo = 'ACENDENTE COM DESCONTO';
      when 'ASCE6' then let cod_vinculo = 'ASCENDENTE ESP RES ATUAL';
      when 'ASCE7' then let cod_vinculo = 'ASCENDENTE ESP RES ATUAL';
      when 'ASCE1' then let cod_vinculo = 'ASCENCENDENTE ESP RES DEPEND';
      when 'ASCE2' then let cod_vinculo = 'ASCENCENDENTE ESP RES DEPEND';
      when 'ASCE9' then let cod_vinculo = 'ASCENCENDENTE ESP RES DEPEND';
      when 'ASCE3' then let cod_vinculo = 'ASCEN RESOLUÇÃO ANTIGA';
      when 'ASCES' then let cod_vinculo = 'ASCEN RESOLUÇÃO ANTIGA';
      when 'COMPM' then let cod_vinculo = 'COMPANHEIRA(O) MASTER';
      when 'COMPA' then let cod_vinculo = 'COMPANHEIRA(O)';
      when 'COMPS' then let cod_vinculo = 'COMPANHEIRA(O)';
      when 'CONJ'  then let cod_vinculo = 'CONJUGE';
      when 'CONST' then let cod_vinculo = 'CONJUGE ISENTO';
      when 'STAXA' then let cod_vinculo = 'DEPENDENTE SEM TAXA';
      when 'FIMAS' then let cod_vinculo = 'FILHA MAIOR MASTER';
      when 'FILF4' then let cod_vinculo = 'FILHA';
      when 'FILF6' then let cod_vinculo = 'FILHA';
      when 'FILF2' then let cod_vinculo = 'FILHA';
      when 'FILF5' then let cod_vinculo = 'FILHA';
      when 'FILF0' then let cod_vinculo = 'FILHA';
      when 'FILF1' then let cod_vinculo = 'FILHA';
      when 'FILF3' then let cod_vinculo = 'FILHA MAIOR SOLTEIRA';
      when 'FILSE' then let cod_vinculo = 'FILHA SEPARADA';
      when 'FESP'  then let cod_vinculo = 'FILHO ESPECIAL';
      when 'FILM0' then let cod_vinculo = 'FILHO';
      when 'FILM1' then let cod_vinculo = 'FILHO';
      when 'FILM4' then let cod_vinculo = 'FILHO';
      when 'FILM2' then let cod_vinculo = 'FILHO';
      when 'FILM3' then let cod_vinculo = 'FILHO';
      when 'FILM5' then let cod_vinculo = 'FILHO';
      when 'INTE1' then let cod_vinculo = 'INTERCAMBISTA';
      when 'INTE2' then let cod_vinculo = 'INTERCAMBISTA';
      when 'INTE3' then let cod_vinculo = 'INTERCAMBISTA';
      when 'MDEDE' then let cod_vinculo = 'MAIOR DEPENDENTE CURATELA';
      when 'MDEPE' then let cod_vinculo = 'MAIOR DEPENDENTE CURATELA';
      when 'MAIMP' then let cod_vinculo = 'MAIOR DEPENDENTE CURATELA';
      when 'TUTEL' then let cod_vinculo = 'MENOR TUTELA/GUARDA';
      when 'NETA'  then let cod_vinculo = 'NETA';
      when 'NETO'  then let cod_vinculo = 'NETO';
      when 'TIT'   then let cod_vinculo = 'TITULAR';
      else let cod_vinculo = cod_parentesco;
   end case;
   return cod_vinculo;
end function;
"""

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

    Chave = recordtype('Chave', 'cod_clube, cod_parentesco')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_ifx.execute(f"""
            select
                '{cod_clube}' || '|' || idt_parentesco as pk_vinculo
            from {linha_log.banco}:taxa_parentesco as taxa_parentesco
            where
                cod_parentesco = ?
        """,(
            chave.cod_parentesco,
        ))

        pk_vinculo = cr_ifx.fetchone()[0]

        cr_sql.execute(f"""
            delete from TaxaDependente
            where
                IdTaxaDependente = (select PkSql from PkDePara where Tabela = 'TaxaDependente' and PkIfx = ?)
        """, (
            linha_log.pk
        ))


        cr_sql.execute(f"""
            delete from Vinculo
            where
                IdVinculo = (select PkSql from PkDePara where Tabela = 'IdVinculo' and PkIfx = ?)
        """, (
            pk_vinculo
        ))

        cr_sql.close()

        return

    cr_ifx.execute(f"""
        select *,'{cod_clube}' || '|' || nome_vinculo as pk_vinculo
        from
        (
            select
                '{cod_clube}' as cod_clube,
                cod_vinculo(cod_parentesco) as nome_vinculo,
                idt_comprovacao = 'S' as idt_comprovacao,
                case idt_sexo
                    when 'M' then 'Masculino'
                    when 'F' then 'Feminino'
                    when 'A' then 'Ambos'
                end as idt_sexo,
                min_idade,
                max_idade,
                vlr_taxa,
                case when des_parentesco matches 'NAO USAR*' then today end as dat_desativacao
            from {linha_log.banco}:taxa_parentesco as taxa_parentesco
            where
                cod_parentesco = ?
        )
    """,(
        chave.cod_parentesco,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    if not origem:
        return

    # --------------------------------------- Vinculo ---------------------------------------------------------
    cr_sql.execute("""
        update Vinculo set
            IdClube = ?,
            NomeVinculo = ?,
            ExigeComprovacao = ?,
            UltimaAlteracao = getdate()
        where
            IdVinculo = (select PkSql from PkDePara where Tabela = 'Vinculo' and PkIfx = ?)
    """,(
        origem.cod_clube,
        origem.nome_vinculo,
        origem.idt_comprovacao,
        origem.pk_vinculo
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Vinculo
            (
                IdClube,
                NomeVinculo,
                ExigeComprovacao
            ) output inserted.IdVinculo  values (
                ? /*IdClube*/,
                ? /*NomeVinculo*/,
                ? /*ExigeComprovacao*/
            )
        """,(
            origem.cod_clube,
            origem.nome_vinculo,
            origem.idt_comprovacao
        ))

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Vinculo',?,?)",(pkSql, origem.pk_vinculo,))
        cr_sql.execute("commit transaction")

    # --------------------------------------- TaxaDependente ---------------------------------------------------------

    cr_sql.execute("""
        update TaxaDependente set
            IdVinculo = (select PkSql from PkDePara where Tabela = 'Vinculo' and PkIfx = ?),
            Sexo = ?,
            IdadeMinima = ?,
            IdadeMaxima = ?,
            ValorTaxa = ?,
            DataDesativacao = ?
        where
            IdTaxaDependente = (select PkSql from PkDePara where Tabela = 'TaxaDependente' and PkIfx = ?)
    """,(
            origem.pk_vinculo,
            origem.idt_sexo,
            origem.min_idade,
            origem.max_idade,
            origem.vlr_taxa,
            origem.dat_desativacao,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into TaxaDependente
            (
                IdVinculo,
                Sexo,
                IdadeMinima,
                IdadeMaxima,
                ValorTaxa,
                DataDesativacao
            ) values (
                (select PkSql from PkDePara where Tabela = 'Vinculo' and PkIfx = ?) /*IdVinculo*/,
                ? /*Sexo*/,
                ? /*IdadeMinima*/,
                ? /*IdadeMaxima*/,
                ? /*ValorTaxa*/,
                ? /*DataDesativacao*/
            )
        """,(
            origem.pk_vinculo,
            origem.idt_sexo,
            origem.min_idade,
            origem.max_idade,
            origem.vlr_taxa,
            origem.dat_desativacao,
        ))

        cr_sql.execute("""select ident_current('TaxaDependente')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('TaxaDependente',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'taxa_parentesco'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
