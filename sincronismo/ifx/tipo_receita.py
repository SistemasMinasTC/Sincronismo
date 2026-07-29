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

    Chave = recordtype('Chave', 'cod_tipo_receita')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from TipoReceita
            where
                IdTipoReceita = (select PkSql from PkDePara where Tabela = 'TipoReceita' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            cod_tipo_receita, 
            des_tipo_receita,
            trim
            (
                case idt_receita
                    when 'E' then 'Educação'
                    when 'S' then 'Sociais'
                    when 'O' then 'Outros'
                    when 'P' then 'Serviços'
                    else 'Outros'
                end
            ) as nom_grupo_receita
        from {linha_log.banco}:tipo_receita as tipo_receita
        where
            cod_tipo_receita = ?
    """,(
        chave.cod_tipo_receita,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update TipoReceita set
            NomeTipoReceita = ?,
            GrupoReceita = ?,
            CodigoTipoReceita = ?,
            UltimaAlteracao = getdate()
        where
            IdTipoReceita = (select PkSql from PkDePara where Tabela = 'TipoReceita' and PkIfx = ?)
    """,(
            origem.des_tipo_receita,
            origem.nom_grupo_receita,
            origem.cod_tipo_receita, 
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute("""
            insert into TipoReceita
            (
                NomeTipoReceita,
                GrupoReceita, 
                CodigoTipoReceita, 
            ) values (
                ? /*NomeTipoReceita*/,
                ? /*GrupoReceita*/, 
                ? /*CodigoTipoReceita*/
            )
        """,(
            origem.des_tipo_receita,
            origem.nom_grupo_receita, 
            origem.cod_tipo_receita
        ))

    cr_sql.close()

# Teste
#
if __name__ == "__main__":
    import sys

    from conexoes import *

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
            tabela = 'tipo_receita'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
