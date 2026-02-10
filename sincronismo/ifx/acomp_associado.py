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

    Chave = recordtype('Chave', 'cod_clube, cod_associado, nro_seq_acomp')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from AcompanhanteAssociado
            where
                IdAcompanhanteAssociado = (select PkSql from PkDePara where Tabela = 'AcompanhanteAssociado' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.execute(f"""
            delete from PkDePara
            where
               Tabela = 'AcompanhanteAssociado' and
               PkIfx = ?
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            '{cod_clube}|' || acomp_associado.nro_seq_acomp as nro_seq_acomp,
            acomp_associado.cod_associado,
            cod_tipo_prior as cod_tipo_associado,
            cod_cota_prior as cod_cota
        from {linha_log.banco}:acomp_associado as acomp_associado
        inner join {linha_log.banco}:associado as associado on associado.cod_associado = acomp_associado.cod_associado
        where
            acomp_associado.cod_associado = ? and
            acomp_associado.nro_seq_acomp = ?
    """,(
        chave.cod_associado,
        chave.nro_seq_acomp,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    if not origem:
        raise Exception('Acompanhante não encontrado no Informix')

    # Busca dados do associado em ordem da última admissão
    #
    cr_sql.execute("""
        select Associado.IdAssociado
        from Associado
        inner join Cota on Cota.IdCota = Associado.IdCota
        where
           Cota.IdClube = ? and
           Associado.NPF = ? and
           Cota.TipoCota = ? and
           Cota.NumeroCota = ?
        order by DataAdmissao desc
    """, (
        origem.cod_clube,
        origem.cod_associado,
        origem.cod_tipo_associado,
        origem.cod_cota
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    dados = Linha(*linha) if linha else None

    if not dados:
        raise Exception('Associado não encontrado no MinasCorporativo')

    cr_sql.execute("""
        update AcompanhanteAssociado set
            IdAcompanhante = (select PkSql from PkDePara where Tabela = 'Acompanhante' and PkIfx = ?),
            IdAssociado = ?,
            UltimaAlteracao = getdate()
        where
            IdAcompanhanteAssociado = (select PkSql from PkDePara where Tabela = 'AcompanhanteAssociado' and PkIfx = ?)
    """,(
            origem.nro_seq_acomp,
            dados.IdAssociado,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into AcompanhanteAssociado
            (
                IdAcompanhante,
                IdAssociado
            ) values (
                (select PkSql from PkDePara where Tabela = 'Acompanhante' and PkIfx = ?) /*IdAcompanhante*/,
                ? /*IdAssociado*/
            )
        """,(
            origem.nro_seq_acomp,
            dados.IdAssociado
        ))

        cr_sql.execute("""select ident_current('AcompanhanteAssociado')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('AcompanhanteAssociado',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'acomp_associado'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
