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
    
    Chave = recordtype('Chave', 'cod_clube, idf_associado_servico')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from ServicoAssociado
            where
                IdServicoAssociado = (select PkSql from PkDePara where Tabela = 'ServicoAssociado' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select 
            '{cod_clube}' as cod_clube,
            idf_tipo_servico,
            nro_controle,
            dat_inicio,
            dat_fim,
            cod_usuario,
            dat_ultima_intervencao
        from associado_servico
        where
            idf_associado_servico = ?
    """,(
        chave.idf_associado_servico,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update ServicoAssociado set
            IdAssociado = (select PkSql from PkDePara where Tabela = 'Associado' and PkIfx = ?),,
            IdServicoTipo = (select PkSql from PkDePara where Tabela = 'ServicoTipo' and PkIfx = ?),,
            NumeroControle = ?,
            DataInicio = ?,
            DataFim = ?,
            IdUsuario = ?,
            UltimaAlteracao = ?,
            UltimaAlteracao = getdate()
        where
            IdServicoAssociado = (select PkSql from PkDePara where Tabela = 'ServicoAssociado' and PkIfx = ?)
    """,(
            origem.cod_clube,
            origem.idf_tipo_servico,
            origem.nro_controle,
            origem.dat_inicio,
            origem.dat_fim,
            origem.cod_usuario,
            origem.dat_ultima_intervencao,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into ServicoAssociado
            (
                IdAssociado,
                IdServicoTipo,
                NumeroControle,
                DataInicio,
                DataFim,
                IdUsuario,
                UltimaAlteracao
            ) output inserted.IdServicoAssociado values (
                (select PkSql from PkDePara where Tabela = 'Associado' and PkIfx = ?) /*IdAssociado*/,
                (select PkSql from PkDePara where Tabela = 'ServicoTipo' and PkIfx = ?) /*IdServicoTipo*/,
                ? /*NumeroControle*/,
                ? /*DataInicio*/,
                ? /*DataFim*/,
                ? /*IdUsuario*/,
                ? /*UltimaAlteracao*/
            )
        """,(
            origem.cod_clube,
            origem.idf_tipo_servico,
            origem.nro_controle,
            origem.dat_inicio,
            origem.dat_fim,
            origem.cod_usuario,
            origem.dat_ultima_intervencao
        ))

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('ServicoAssociado',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'associado_servico'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
