#!/usr/bin/python
#

from recordtype import recordtype

def convert(conn_ifx, conn_sql, linha_log):
    cr_sql = conn_sql.cursor()
    try:
        cr_sql.execute('create table #sincronizando (dummy char(1))')
    except:
        pass

    if linha_log.banco == 'minas':
        cod_clube = 'MTC'
    elif linha_log.banco == 'nautico':
        cod_clube = 'MTNC'

    linha_log.pk = cod_clube+'|CT|'+linha_log.pk

    cr_ifx = conn_ifx.cursor()
    cr_ifx.execute('execute procedure em_sincronismo()')

    Chave = recordtype('Chave', 'cod_clube, cod_tipo_associado, cod_tipo_contrib')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from ClasseCota
            where
                IdClasseCota = (select PkSql from PkDePara where Tabela = 'ClasseCota' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            'MTC' as cod_clube,
            'CT' as cod_tipo_associado,
            des_tipo_contrib,
            cod_tipo_contrib
        from {linha_log.banco}:tipo_contribuinte as tipo_contribuinte
        where
            cod_tipo_contrib = ?
    """,(
        chave.cod_tipo_contrib,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update ClasseCota set
            IdClube = ?,
            TipoCota = ?,
            Nome = ?,
            CodigoClasse = ?,
            UltimaAlteracao = getdate()
        where
            IdClasseCota = (select PkSql from PkDePara where Tabela = 'ClasseCota' and PkIfx = ?)
    """,(
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.des_tipo_contrib,
            origem.cod_tipo_contrib,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into ClasseCota
            (
                IdClube,
                TipoCota,
                Nome,
                CodigoClasse
            ) output inserted.IdClasseCota  values (
                ? /*IdClube*/,
                ? /*TipoCota*/,
                ? /*Nome*/,
                ? /*CodigoClasse*/
            )
        """,(
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.des_tipo_contrib,
            origem.cod_tipo_contrib
        ))

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('ClasseCota',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'tipo_contribuinte'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
