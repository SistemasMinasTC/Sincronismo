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

    Chave = recordtype('Chave', 'cod_nivel')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Nivel
            where
                IdNivel = (select PkSql from PkDePara where Tabela = 'Nivel' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            des_nivel,
            nro_ordem
        from {linha_log.banco}:nivel as nivel
        where
            cod_nivel = ?
    """,(
        chave.cod_nivel,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Nivel set
            NomeNivel = ?,
            Ordem = ?,
            UltimaAlteracao = getdate()
        where
            IdNivel = (select PkSql from PkDePara where Tabela = 'Nivel' and PkIfx = ?)
    """,(
            origem.des_nivel,
            origem.nro_ordem,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Nivel
            (
                NomeNivel,
                Ordem
            ) output values (
                ? /*NomeNivel*/,
                ? /*Ordem*/
            )
        """,(
            origem.des_nivel,
            origem.nro_ordem
        ))

        cr_sql.execute("""select ident_current('Nivel')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Nivel',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'nivel'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
