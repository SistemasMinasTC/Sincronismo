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

    Chave = recordtype('Chave', 'cod_motivo')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Motivo
            where
                CodigoMotivo = ?
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            cod_motivo,
            des_motivo
        from {linha_log.banco}:mot_movto as mot_movto
        where
            cod_motivo = ?
    """,(
        chave.cod_motivo,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Motivo set
            CodigoMotivo = ?,
            Descricao = ?,
            UltimaAlteracao = getdate()
        where
            IdMotivo = (select PkSql from PkDePara where Tabela = 'Motivo' and PkIfx = ?)
    """,(
            origem.cod_motivo,
            origem.des_motivo,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Motivo
            (
                CodigoMotivo,
                Descricao
            ) values (
                ? /*CodigoMotivo*/,
                ? /*Descricao*/
            )
        """,(
            origem.cod_motivo,
            origem.des_motivo
        ))


        cr_sql.execute("""select ident_current('Motivo')""")

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Motivo',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'mot_movto'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
