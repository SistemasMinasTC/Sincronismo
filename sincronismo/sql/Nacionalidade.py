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
            IdNacionalidade,
            Pais,
            Nome,
            concat(IdNacionalidade,null) as PkIfx
        from Nacionalidade
        where
            IdNacionalidade = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.close()

    if not origem:
        return

    Chave = recordtype('Chave', 'cod_nacionalidade')
    chave = Chave(*origem.PkIfx.split('|'))

    linha_log.banco = 'minas' if 'cod_clube' not in chave._fields else 'nautico' if chave.cod_clube == 'MTNC' else 'minas'

    if linha_log.operacao == 'del':
        cr_ifx.execute(f"""
            delete from {linha_log.banco}:nacionalidade
            where
                cod_nacionalidade = ?
        """, (
            chave.cod_nacionalidade,
        ))

        return

    cr_ifx.execute(f"""
        update {linha_log.banco}:nacionalidade set
            nom_pais = ?,
            nom_nacionalidade = ?
        where
            cod_nacionalidade = ?
    """,(
            origem.Pais,
            origem.Nome,
            chave.cod_nacionalidade,
    ))

    if cr_ifx.rowcount == 0:
        cr_ifx.execute(f"""
            insert into {linha_log.banco}:nacionalidade
            (
                cod_nacionalidade,
                nom_pais,
                nom_nacionalidade
            ) values (
                ? {{cod_nacionalidade}},
                ? {{nom_pais}},
                ? {{nom_nacionalidade}}
            )
        """,(
            origem.IdNacionalidade,
            origem.Pais,
            origem.Nome
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
            tabela = 'Nacionalidade'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)


