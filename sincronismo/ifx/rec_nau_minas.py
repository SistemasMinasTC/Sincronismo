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

    if linha_log.banco == 'minas':
        cod_clube = 'MTC'
    elif linha_log.banco == 'nautico':
        cod_clube = 'MTNC'
    elif linha_log.banco == 'serra':
        cod_clube = 'MSDR'

    linha_log.pk = cod_clube+'|'+linha_log.pk

    Chave = recordtype('Chave', 'cod_clube, idf_naumin')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from EquivalenciaReceita
            where
                IdEquivalenciaReceita = (select PkSql from PkDePara where Tabela = 'EquivalenciaReceita' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            cod_rec_nautico,
            cod_rec_minas
        from {linha_log.banco}:rec_nau_minas as rec_nau_minas
        where
            idf_naumin = ?
    """,(
        chave.idf_naumin,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    if not origem:
        cr_sql.close()
        return

    cr_sql.execute("""
        update EquivalenciaReceita set
            UltimaAlteracao = getdate()
        where
            IdReceitaOrigem = (select IdReceita from Receita where IdClube = ? and CodigoReceita = ?) and
            IdReceitaDestino = (select IdReceita from Receita where IdClube = 'MTC' and CodigoReceita = ?)
    """,(
            cod_clube,
            origem.cod_rec_nautico,
            origem.cod_rec_minas,
    ))

    if cr_sql.rowcount == 0:

        cr_sql.execute(f"""
            insert into EquivalenciaReceita
            (
                IdReceitaOrigem,
                IdReceitaDestino
            ) values (
                (select IdReceita from Receita where IdClube = ? and CodigoReceita = ?) /*IdReceitaOrigem*/,
                (select IdReceita from Receita where IdClube = 'MTC' and CodigoReceita = ?) /*IdReceitaDestino*/
            )
        """,(
            cod_clube,
            origem.cod_rec_nautico,
            origem.cod_rec_minas
        ))

    cr_sql.close()

# Teste
#
if __name__ == "__main__":
    import sys
    from conexoes import *

    ifx = conecta_informix('nautico')
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
            tabela = 'rec_nau_minas'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
