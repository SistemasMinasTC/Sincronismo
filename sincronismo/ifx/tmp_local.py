#/usr/bin/python
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

    Chave = recordtype('Chave', 'cod_clube, cod_unidade, nro_seq_local')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from Local
            where
                IdLocal = (select PkSql from PkDePara where Tabela = 'Local' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}|' || cod_unidade as cod_unidade,
            nom_local,
            trim
            (
                case
                    when idt_local = '0' then 'OUTROS'
                    when idt_local = 'E' then 'OUTROS'
                    when idt_local = 'G' then 'OUTROS'
                    when idt_local = 'L' then 'OUTROS'
                    when idt_local = 'O' then 'OUTROS'
                    when idt_local = 'P' then 'PISCINA'
                    when idt_local = 'Q' then 'QUADRA'
                    when idt_local = 'S' then 'SALA'
                    else 'OUTROS'
                end
            ) as idt_local,
            idc_grupo = 'S' as idc_grupo,
            '{cod_clube}|' || cod_unidade || '|' || nro_seq_loc_subord as nro_seq_loc_subord,
            idc_ativo = 'S' as idc_ativo,
            idc_reserva = 'S' as idc_reserva,
            idc_luminaria = 'S' as idc_luminaria,
            capacidade_maxima
        from {linha_log.banco}:tmp_local as tmp_local
        where
            cod_unidade = ? and
            nro_seq_local = ?
    """,(
        chave.cod_unidade,
        chave.nro_seq_local,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update Local set
            IdUnidade = (select PkSql from PkDePara where Tabela = 'Unidade' and PkIfx = ?),
            NomeLocal = ?,
            IdLocalTipo = (select Top 1 IdLocalTipo from LocalTipo where NomeLocalTipo = ? ),
            GrupoLocais = ?,
            IdLocalSuperior = (select PkSql from PkDePara where Tabela = 'Local' and PkIfx = ?),
            Ativo = ?,
            PermiteReserva = ?,
            TemLuminaria = ?,
            CapacidadeMaxima = ?,
            UltimaAlteracao = getdate()
        where
            IdLocal = (select PkSql from PkDePara where Tabela = 'Local' and PkIfx = ?)
    """,(
            origem.cod_unidade,
            origem.nom_local,
            origem.idt_local,
            origem.idc_grupo,
            origem.nro_seq_loc_subord,
            origem.idc_ativo,
            origem.idc_reserva,
            origem.idc_luminaria,
            origem.capacidade_maxima,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Local
            (
                IdUnidade,
                NomeLocal,
                IdLocalTipo,
                GrupoLocais,
                IdLocalSuperior,
                Ativo,
                PermiteReserva,
                TemLuminaria,
                CapacidadeMaxima
            ) values (
                (select PkSql from PkDePara where Tabela = 'Unidade' and PkIfx = ?) /*IdUnidade*/,
                ? /*NomeLocal*/,
                (select top 1 IdLocalTipo from LocalTipo where NomeLocalTipo = ? ) /*TipoLocal*/,
                ? /*GrupoLocais*/,
                (select PkSql from PkDePara where Tabela = 'Local' and PkIfx = ?) /*IdLocalSuperior*/,
                ? /*Ativo*/,
                ? /*PermiteReserva*/,
                ? /*TemLuminaria*/,
                ? /*CapacidadeMaxima*/
            )
        """,(
            origem.cod_unidade,
            origem.nom_local,
            origem.idt_local,
            origem.idc_grupo,
            origem.nro_seq_loc_subord,
            origem.idc_ativo,
            origem.idc_reserva,
            origem.idc_luminaria,
            origem.capacidade_maxima
        ))

        cr_sql.execute("""select ident_current('Local')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Local',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'tmp_local'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
