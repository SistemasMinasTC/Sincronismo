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

    Chave = recordtype('Chave', 'cod_clube, nro_seq_agregado')
    chave = Chave(*linha_log.pk.split('|'))

    cr_ifx.execute("""
        select
            'MTC' as cod_clube, 
            agregado.cod_tipo_associado, 
            agregado.cod_cota,
            agregado.dat_inicio,
            agregado.idc_cobra_taxa = 'S' as idc_cobra_taxa,
            agregado.dat_cancel,
            agregado.cod_empresa
        from minas:agregado as agregado
        where
            nro_seq_agregado = ?
    """,(
        chave.nro_seq_agregado,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None
    
    # busca cota correspondente no clube agregado
    #
    cr_ifx.execute(f"""
        select min(cod_agregado) as cod_agregado
        from {'nautico' if origem.cod_empresa == 'MTNC' else 'serra'}:agreg_nautico
        where 
            cod_tipo_associado = ? and
            cod_cota = ? and 
            dat_inicio = ?
        """,(
            origem.cod_tipo_associado, 
            origem.cod_cota, 
            origem.dat_inicio
        ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    agreg = Linha(*linha) if linha else None
    
        
    if linha_log.operacao == 'del':
        if origem:
            cr_sql.execute("""
                delete from Adesao
                where
                    IdCota = (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?) and
                    IdCotaAdesao = (select IdCota from Cota where IdClube = ? and TipoCota = 'CC' and NumeroCota = ?)
                    DataInicio = ?
            """, (
                origem.cod_clube,
                origem.cod_tipo_associado,
                origem.cod_cota,
                origem.cod_empresa,
                agregado.cod_agregado, 
                origem.dat_inicio,
            ))

        cr_sql.close()
        return

    if not origem:
        cr_sql.close()
        return
        
    cr_sql.execute('''
        select IdCota 
        from Cota 
        where 
            IdClube = ? and
            TipoCota = 'CC' and
            NumeroCota = ?
    ''',(
        origem.cod_empresa, 
        agreg.cod_agregado, 
    ))
    
    IdCotaAdesao = cr_sql.fetchval()
    
    if not IdCotaAdesao:
        raise ValueError(f"IdCotaAdesao não encontrada para cod_agregado: {agreg.cod_agregado}")
        
    cr_sql.execute("""
        update Adesao set
            CobraTaxa = ?,
            DataCancelamento = ?,
            UltimaAlteracao = getdate()
        where
            IdCota = (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?)  and
            IdCotaAdesao = ? and
            DataInicio = ?
    """,(
        origem.idc_cobra_taxa,
        origem.dat_cancel,
        origem.cod_clube,
        origem.cod_tipo_associado,
        origem.cod_cota,
        IdCotaAdesao,
        origem.dat_inicio,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute("""
            insert into Adesao
            (
                IdCota,
                DataInicio,
                CobraTaxa,
                DataCancelamento,
                IdCotaAdesao
            ) values (
                (select IdCota from Cota where IdClube = ? and TipoCota = ? and NumeroCota = ?) /*Cota*/,
                ? /*DataInicio*/,
                ? /*CobraTaxa*/,
                ? /*DataCancelamento*/,
                ? /*CotaAdesao*/
            )
        """,(
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.cod_cota,
            origem.dat_inicio,
            origem.idc_cobra_taxa,
            origem.dat_cancel,
            IdCotaAdesao
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
            tabela = 'agregado' 
        order by data_hora
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
