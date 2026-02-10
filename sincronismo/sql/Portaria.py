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

    cr_sql.execute("""
        select
            IdUnidade,
            IdPortaria,
            NomePortaria,
            PkDePara.PkIfx
        from Portaria
        left join PkDePara on
            Tabela = 'Portaria' and
            PkDePara.PkSql = IdPortaria
        where
            IdPortaria = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.close()

    if not origem:
        return

    Chave = recordtype('Chave', 'cod_unidade,cod_portaria')
    chave = Chave(*origem.PkIfx.split('|'))

    linha_log.banco = 'minas' if 'cod_clube' not in chave._fields else 'nautico' if chave.cod_clube == 'MTNC' else 'minas'

    if linha_log.operacao == 'del':
        cr_ifx.execute(f"""
            delete from {linha_log.banco}:portaria
            where
                cod_unidade = ? and
            cod_portaria = ?
        """, (
            chave.cod_unidade,
        chave.cod_portaria,
        ))

        return

    cr_ifx.execute(f"""
        update {linha_log.banco}:portaria set
            des_portaria = ?
        where
            cod_unidade = ? and
            cod_portaria = ?
    """,(
            origem.NomePortaria,
            chave.cod_unidade,
        chave.cod_portaria,
    ))

    if cr_ifx.rowcount == 0:
        cr_ifx.execute(f"""
            insert into {linha_log.banco}:portaria
            (
                cod_unidade,
                cod_portaria,
                des_portaria
            ) values (
                ? {cod_unidade},
                ? {cod_portaria},
                ? {des_portaria}
            )
        """,(
            origem.IdUnidade,
            origem.IdPortaria,
            origem.NomePortaria
        ))

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
            tabela = 'Portaria'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)

