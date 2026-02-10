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

    Chave = recordtype('Chave', 'cod_usuario')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Usuario
            where
                IdUsuario = (select PkSql from PkDePara where Tabela = 'Usuario' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            trim
            (
                case
                    when idf_cidadao in (select idf_cidadao from minas:associado) then 'Associado|' || (select min(cod_associado) from minas:associado where idf_cidadao = usuario.idf_cidadao)
                    when idf_cidadao in (select idf_cidadao from nautico:associado) then 'Associado|' || (select min(cod_associado) from nautico:associado where idf_cidadao = usuario.idf_cidadao)
                    else 'Cidadao|' || idf_cidadao
                end
            ) as idf_cidadao,
            cod_usuario,
            mla_func,
            idt_ativo = 'S' as idt_ativo
        from {linha_log.banco}:usuario as usuario
        where
            cod_usuario = ?
    """,(
        chave.cod_usuario,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_ifx.execute(f"""
        insert into mc_log
        (
            data_hora,
            banco,
            tabela,
            operacao,
            pk
        ) values (current,'minasdba','cidadao','upd','{origem.idf_cidadao.split('|')[1]}')
    """)

    cr_sql.execute("""
        update Usuario set
            IdPessoa = (select PkSql from PkDePara where Tabela = 'Pessoa' and PkIfx = ?),
            Login = ?,
            Matricula = ?,
            Ativo = ?,
            UltimaAlteracao = getdate()
        where
            IdUsuario = (select PkSql from PkDePara where Tabela = 'Usuario' and PkIfx = ?)
    """,(
            origem.idf_cidadao,
            origem.cod_usuario,
            origem.mla_func,
            origem.idt_ativo,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Usuario
            (
                IdPessoa,
                Login,
                Matricula,
                Ativo
            ) output inserted.IdUsuario  values (
                (select PkSql from PkDePara where Tabela = 'Pessoa' and PkIfx = ?) /*IdPessoa*/,
                ? /*Login*/,
                ? /*Matricula*/,
                ? /*Ativo*/
            )
        """,(
            origem.idf_cidadao,
            origem.cod_usuario,
            origem.mla_func,
            origem.idt_ativo,
        ))

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Usuario',?,?)",(pkSql, linha_log.pk,))
        cr_sql.execute("commit transaction")

    cr_sql.close()

# Teste
#
if __name__ == "__main__":
    import sys
    from biblioteca.conexoes import *

    ifx = conecta_informix('acesso')
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
            tabela = 'usuario'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
