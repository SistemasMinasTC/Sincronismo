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

    Chave = recordtype('Chave', 'cod_atestado')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from AtestadoMedico
            where
                IdAtestadoMedico = (select PkSql from PkDePara where Tabela = 'AtestadoMedico' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            des_atestado,
            idc_aceita_externo = 'S' as idc_aceita_externo,
            qtd_max_dias,
            idc_exige_renova = 'S' as idc_exige_renova,
            idc_ativo = 'S' as idc_ativo,
            idc_imagem_obrigatoria = 'S' as idc_imagem_obrigatoria
        from {linha_log.banco}:atestado as atestado
        where
            cod_atestado = ?
    """,(
        chave.cod_atestado,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update AtestadoMedico set
            NomeAtestado = ?,
            AceitaAtestadoExterno = ?,
            DiasVigencia = ?,
            ExigeRenovacao = ?,
            Ativo = ?,
            ImagemObrigatoria = ?,
            UltimaAlteracao = getdate()
        where
            IdAtestadoMedico = (select PkSql from PkDePara where Tabela = 'AtestadoMedico' and PkIfx = ?)
    """,(
            origem.des_atestado,
            origem.idc_aceita_externo,
            origem.qtd_max_dias,
            origem.idc_exige_renova,
            origem.idc_ativo,
            origem.idc_imagem_obrigatoria,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into AtestadoMedico
            (
                NomeAtestado,
                AceitaAtestadoExterno,
                DiasVigencia,
                ExigeRenovacao,
                Ativo,
                ImagemObrigatoria
            ) values (
                ? /*NomeAtestado*/,
                ? /*AceitaAtestadoExterno*/,
                ? /*DiasVigencia*/,
                ? /*ExigeRenovacao*/,
                ? /*Ativo*/,
                ? /*ImagemObrigatoria*/
            )
        """,(
            origem.des_atestado,
            origem.idc_aceita_externo,
            origem.qtd_max_dias,
            origem.idc_exige_renova,
            origem.idc_ativo,
            origem.idc_imagem_obrigatoria
        ))

        cr_sql.execute("""select ident_current('AtestadoMedico')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('AtestadoMedico',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'atestado'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
