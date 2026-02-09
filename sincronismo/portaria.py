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

    linha_log.pk = cod_clube+'|'+linha_log.pk

    Chave = recordtype('Chave', 'cod_clube, cod_unidade, cod_portaria')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Portaria
            where
                IdPortaria = (select PkSql from PkDePara where Tabela = 'Portaria' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}|' || cod_unidade as cod_unidade,
            des_portaria,
            ip_micro
        from {linha_log.banco}:portaria as portaria
        where
            cod_unidade = ? and
            cod_portaria = ?
    """,(
        chave.cod_unidade,
        chave.cod_portaria,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Portaria set
            IdUnidade = (select PkSql from PkDePara where Tabela = 'Unidade' and PkIfx = ?),
            NomePortaria = ?,
            NumeroIp = ?,
            UltimaAlteracao = getdate()
        where
            IdPortaria = (select PkSql from PkDePara where Tabela = 'Portaria' and PkIfx = ?)
    """,(
            origem.cod_unidade,
            origem.des_portaria,
            origem.ip_micro,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Portaria
            (
                IdUnidade,
                NomePortaria,
                NumeroIp
            ) values (
                (select PkSql from PkDePara where Tabela = 'Unidade' and PkIfx = ?) /*IdUnidade*/,
                ? /*NomePortaria*/,
                ? /*NumeroIp*/
            )
        """,(
            origem.cod_unidade,
            origem.des_portaria,
            origem.ip_micro
        ))

        cr_sql.execute("""select ident_current('Portaria')""")

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Portaria',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'portaria'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
