#!/usr/bin/python
#

from recordtype import recordtype

def get_pk(cr, tabela, pk_ifx):
    row = cr.execute(
        "SELECT PkSql FROM PkDePara WHERE Tabela=? AND PkIfx=?",
        (tabela, pk_ifx)
    ).fetchone()

    #IMPORTANTE: garantir retorno como INT (evita erro de tipo no pyodbc)
    return int(row[0]) if row and row[0] is not None else None


#Função para normalizar tipos (ESSENCIAL para evitar HYC00)
def normalize(val):
    if val is None:
        return None

    # bool → int
    if isinstance(val, bool):
        return 1 if val else 0

    # date/datetime → string
    if hasattr(val, 'strftime'):
        return val.strftime('%Y-%m-%d %H:%M:%S')

    # string numérica → int
    if isinstance(val, str) and val.isdigit():
        return int(val)

    return val


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

    linha_log.pk = cod_clube + '|' + linha_log.pk

    Chave = recordtype('Chave', 'cod_clube, nro_seq_agregado')
    chave = Chave(*linha_log.pk.split('|'))

    
    if linha_log.operacao == 'del':
        id_adesao = get_pk(cr_sql, 'Adesao', linha_log.pk)

        if id_adesao:
            cr_sql.execute(
                "DELETE FROM Adesao WHERE IdAdesao = ?",
                (id_adesao,)
            )

        cr_sql.close()
        return

   
    cr_ifx.execute("""
        select
            'MTC|' || agregado.cod_tipo_associado ||'|'|| agregado.cod_cota as cod_cota,
            agregado.dat_inicio,
            agregado.idc_cobra_taxa = 'S' as idc_cobra_taxa,
            agregado.dat_cancel,
            'MTNC|CC|' || cod_agregado as cod_cota_agreg 
        from Minas:agregado as agregado
        inner join nautico:agreg_nautico nautico 
            on nautico.cod_cota = agregado.cod_cota
        where
            nro_seq_agregado = ?
    """, (chave.nro_seq_agregado,))

    Linha = recordtype('Linha', [col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    if not origem:
        cr_sql.close()
        return

    id_cota = get_pk(cr_sql, 'Cota', origem.cod_cota)
    id_cota_adesao = get_pk(cr_sql, 'Cota', origem.cod_cota_agreg)
    id_adesao = get_pk(cr_sql, 'Adesao', linha_log.pk)

    if id_cota is None:
        print("Cota não encontrada:", origem.cod_cota)
        cr_sql.close()
        return

    params_update = (
        normalize(id_cota),
        normalize(origem.dat_inicio),
        normalize(origem.idc_cobra_taxa),
        normalize(origem.dat_cancel),
        normalize(id_cota_adesao),
        normalize(id_adesao)
    )

    if id_adesao:
        cr_sql.execute("""
            UPDATE Adesao SET
                IdCota = ?,
                DataInicio = ?,
                CobraTaxa = ?,
                DataCancelamento = ?,
                IdCotaAdesao = ?,
                UltimaAlteracao = GETDATE()
            WHERE IdAdesao = ?
        """, params_update)


    if not id_adesao:
        cr_sql.execute('BEGIN TRANSACTION')

        params_insert = (
            normalize(id_cota),
            normalize(origem.dat_inicio),
            normalize(origem.idc_cobra_taxa),
            normalize(origem.dat_cancel),
            normalize(id_cota_adesao)
        )

        cr_sql.execute("""
            INSERT INTO Adesao
            (
                IdCota,
                DataInicio,
                CobraTaxa,
                DataCancelamento,
                IdCotaAdesao
            )
            OUTPUT INSERTED.IdAdesao
            VALUES (?, ?, ?, ?, ?)
        """, params_insert)

        pkSql = cr_sql.fetchval()

        cr_sql.execute(
            "INSERT INTO PkDePara VALUES ('Adesao', ?, ?)",
            (pkSql, linha_log.pk)
        )

        cr_sql.execute('COMMIT TRANSACTION')

    cr_sql.close()


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

    Linha = recordtype('Linha', [col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)