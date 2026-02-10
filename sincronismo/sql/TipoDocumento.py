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

    cr_sql.execute("""select PkIfx from PkDePara where Tabela = 'TipoDocumento' and PkSql = ?""", (linha_log.pk,))
    PkIfx = cr_sql.fetchone()
    PkIfx = PkIfx[0] if PkIfx else None

    Chave = recordtype('Chave', 'idf_tipo_documento')
    chave = Chave(*PkIfx.split('|')) if PkIfx else Chave(*[None for i in Chave._fields])

    linha_log.banco = 'minas' if 'cod_clube' not in chave._fields or chave.cod_clube == 'MTC' else 'nautico'

    if linha_log.operacao == 'del':
        cr_ifx.execute(f"""
            delete from {linha_log.banco}:tipo_documento
            where
                idf_tipo_documento = ?
        """, (
            chave.idf_tipo_documento,
        ))

        cr_sql.execute ("""delete from PkDePara where Tabela = 'TipoDocumento' and PkSql = ?""", (linha_log.pk,))

        cr_sql.close()
        return

    cr_sql.execute("""
        select
            Nome
        from TipoDocumento
        where
            IdTipoDocumento = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.close()

    if not origem:
        return

    cr_ifx.execute(f"""
        update {linha_log.banco}:tipo_documento set
            des_tipo_documento = ?
        where
            idf_tipo_documento = ?
    """,(
            origem.Nome,
            chave.idf_tipo_documento,
    ))

    if cr_ifx.rowcount == 0:
        cr_ifx.execute(f"""
            insert into {linha_log.banco}:tipo_documento
            (
                des_tipo_documento
            ) values (
                ? {{des_tipo_documento}}
            )
        """,(
            origem.Nome,
        ))

        cr_ifx.execute("""select DBINFO('sqlca.sqlerrd1') from dual""")
        PkIfx = cr_ifx.fetchone()[0]

        cr_sql = conn_sql.cursor()
        cr_sql.execute("insert into PkDePara values ('TipoDocumento',?,?)",(linha_log.pk, PkIfx))
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
            tabela = 'TipoDocumento'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha, end = ' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)

