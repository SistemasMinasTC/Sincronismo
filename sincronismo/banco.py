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

    Chave = recordtype('Chave', 'cod_banco')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Banco
            where
                IdBanco = (select PkSql from PkDePara where Tabela = 'Banco' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            cod_banco,
            des_banco
        from {linha_log.banco}:banco as banco
        where
            cod_banco = ?
    """,(
        chave.cod_banco,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Banco set
            NomeBanco = ?,
            UltimaAlteracao = getdate()
        where
            IdBanco = ?
    """,(
            origem.des_banco,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute(f"""
            insert into Banco
            (
                IdBanco,
                NomeBanco
            ) values (
                ? /*IdBanco*/,
                ? /*NomeBanco*/
            )
        """,(
            origem.cod_banco,
            origem.des_banco
        ))

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
            tabela = 'banco'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
