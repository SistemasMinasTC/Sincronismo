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

    Chave = recordtype('Chave', 'cod_clube, nro_fatura, nro_seq')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from ItemFatura
            where
                IdFatura = (select IdFatura from Fatura where IdClube = ? and NumeroFatura = ?) and
                NumeroItem = ?
        """, (
            chave.cod_clube,
            chave.nro_fatura,
            chave.nro_seq
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            nro_fatura,
            nro_seq,
            nvl
            ((
               select vlr_acrescimo
               from {linha_log.banco}:item_fatura as item_fatura
               where
                 item_fatura.nro_fatura = item_fat_eterno.nro_fatura and
                 item_fatura.nro_seq = item_fat_eterno.nro_seq
            ),0) as vlr_acrescimo,
            cod_tipo_associado,
            cod_cota,
            cod_associado,
            cod_receita,
            to_char(dat_receita,'%Y-%m-%d ') || hor_receita as dat_receita
        from {linha_log.banco}:item_fat_eterno as item_fat_eterno
        where
            nro_fatura = ? and
            nro_seq = ?
    """,(
        chave.nro_fatura,
        chave.nro_seq,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    origem = Linha(*linha) if (linha := cr_ifx.fetchone()) else None

    cr_sql.execute("""
        select
            Fatura.IdFatura,
            ReceitaCota.IdReceitaCota
        from Cota
        left join Fatura on
            Fatura.IdCota = Cota.IdCota and
            Fatura.NumeroFatura = ?
        left join Receita on
            Receita.IdClube = Cota.IdClube and
            Receita.CodigoReceita = ?
        left join PkDePara on
            PkDePara.Tabela = 'Pessoa' and
            PkDePara.PkIfx = concat('Associado|',?)
        left join Associado on
            Associado.IdPessoa = PkDePara.PkSql and
            Associado.IdCota = Cota.IdCota
        left join ReceitaCota on
            ReceitaCota.IdAssociado = Associado.IdAssociado and
            ReceitaCota.IdReceita = Receita.IdReceita and
            ReceitaCota.DataReceita = ?
        where
            Cota.IdClube = ? and
            Cota.NumeroCota = ? and
            Cota.TipoCota = ?
    """,(
        origem.nro_fatura,
        origem.cod_receita,
        origem.cod_associado,
        origem.dat_receita,
        origem.cod_clube,
        origem.cod_cota,
        origem.cod_tipo_associado
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    dados = Linha(*linha) if (linha := cr_sql.fetchone()) else None

    cr_sql.execute("""
        update ItemFatura set
            ValorMulta = ? - isnull(ValorJuros,0),
            UltimaAlteracao = getdate()
        where
            IdFatura = ? and
            NumeroItem = ?
    """,(
            origem.vlr_acrescimo,
            dados.IdFatura,
            origem.nro_seq
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute(f"""
            insert into ItemFatura
            (
                IdFatura,
                NumeroItem,
                IdReceitaCota,
                ValorMulta,
                ValorJuros
            )  values (
                ? /*IdFatura*/,
                ? /*NumeroItem*/,
                ? /*IdReceitaCota*/,
                ? /*ValorMulta*/,
                ? /*ValorJuros*/
            )
        """,(
            dados.IdFatura,
            origem.nro_seq,
            dados.IdReceitaCota,
            origem.vlr_acrescimo,
            0
        ))

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
            tabela = 'item_fat_eterno'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha, convert(ifx, sql, linha))
