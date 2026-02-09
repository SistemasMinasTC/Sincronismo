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

    linha_log.pk = cod_clube+'|CC|'+linha_log.pk

    Chave = recordtype('Chave', 'cod_clube, cod_tipo_associado, cod_tipo_concessao')
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
            '{cod_clube}' as cod_clube,
            'CC' as cod_tipo_associado,
            des_tipo_concessao,
            idc_aceita_dep = 'S' as idc_aceita_dep,
            idc_aceita_contrib = 'S' as idc_aceita_contrib,
            idc_titulo = 'S' as idc_titulo,
            idc_pega_convite = 'S' as idc_pega_convite,
            idc_cobra_carteira = 'S' as idc_cobra_carteira,
            idc_pede_motivo = 'S' as idc_pede_motivo,
            idc_cobra_dep = 'S' as idc_cobra_dep,
            cod_tipo_concessao
        from {linha_log.banco}:tipo_concessao as tipo_concessao
        where
            cod_tipo_concessao = ?
    """,(
        chave.cod_tipo_concessao,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update ClasseCota set
            IdClube = ?,
            TipoCota = ?,
            Nome = ?,
            AceitaDependente = ?,
            PodeSerContribuinte = ?,
            ETitulo = ?,
            PegaConvite = ?,
            CobraCarteira = ?,
            PedeMotivo = ?,
            CobraDependente = ?,
            CodigoClasse = ?,
            UltimaAlteracao = getdate()
        where
            IdClasseCota = (select PkSql from PkDePara where Tabela = 'ClasseCota' and PkIfx = ?)
    """,(
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.des_tipo_concessao,
            origem.idc_aceita_dep,
            origem.idc_aceita_contrib,
            origem.idc_titulo,
            origem.idc_pega_convite,
            origem.idc_cobra_carteira,
            origem.idc_pede_motivo,
            origem.idc_cobra_dep,
            origem.cod_tipo_concessao,
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
                AceitaDependente,
                PodeSerContribuinte,
                ETitulo,
                PegaConvite,
                CobraCarteira,
                PedeMotivo,
                CobraDependente,
                CodigoClasse
            ) output inserted.IdClasseCota  values (
                ? /*IdClube*/,
                ? /*TipoCota*/,
                ? /*Nome*/,
                ? /*AceitaDependente*/,
                ? /*PodeSerContribuinte*/,
                ? /*ETitulo*/,
                ? /*PegaConvite*/,
                ? /*CobraCarteira*/,
                ? /*PedeMotivo*/,
                ? /*CobraDependente*/,
                ? /*CodigoClasse*/
            )
        """,(
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.des_tipo_concessao,
            origem.idc_aceita_dep,
            origem.idc_aceita_contrib,
            origem.idc_titulo,
            origem.idc_pega_convite,
            origem.idc_cobra_carteira,
            origem.idc_pede_motivo,
            origem.idc_cobra_dep,
            origem.cod_tipo_concessao
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
            tabela = 'tipo_concessao'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
