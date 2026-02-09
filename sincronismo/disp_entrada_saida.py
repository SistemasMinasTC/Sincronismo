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

    Chave = recordtype('Chave', 'cod_clube, cod_unidade, cod_portaria, nro_seq_disp')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from DispositivoAcesso
            where
                IdDispositivoAcesso = (select PkSql from PkDePara where Tabela = 'DispositivoAcesso' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}'||'|'||cod_unidade||'|'||cod_portaria as cod_portaria,
            des_dispositivo,
            idc_posicao_tela,
            idc_sensor_dsp = 'S' as idc_sensor_dsp,
            nro_serie,
            idc_status,
            ip_dispositivo
        from {linha_log.banco}:disp_entrada_saida as disp_entrada_saida
        where
            cod_unidade = ? and
            cod_portaria = ? and
            nro_seq_disp = ?
    """,(
        chave.cod_unidade,
        chave.cod_portaria,
        chave.nro_seq_disp,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update DispositivoAcesso set
            IdPortaria = (select PkSql from PkDePara where Tabela = 'Portaria' and PkIfx = ?),
            NomeDispositivo = ?,
            PosicaoTela = ?,
            TemSensorDigital = ?,
            NumeroSerie = ?,
            Status = ?,
            IpDispositivo = ?,
            UltimaAlteracao = getdate()
        where
            IdDispositivoAcesso = (select PkSql from PkDePara where Tabela = 'DispositivoAcesso' and PkIfx = ?)
    """,(
            origem.cod_portaria,
            origem.des_dispositivo,
            origem.idc_posicao_tela,
            origem.idc_sensor_dsp,
            origem.nro_serie,
            origem.idc_status,
            origem.ip_dispositivo,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into DispositivoAcesso
            (
                IdPortaria,
                NomeDispositivo,
                PosicaoTela,
                TemSensorDigital,
                NumeroSerie,
                Status,
                IpDispositivo
            ) values (
                (select PkSql from PkDePara where Tabela = 'Portaria' and PkIfx = ?) /*IdPortaria*/,
                ? /*NomeDispositivo*/,
                ? /*PosicaoTela*/,
                ? /*TemSensorDigital*/,
                ? /*NumeroSerie*/,
                ? /*Status*/,
                ? /*IpDispositivo*/
            )
        """,(
            origem.cod_portaria,
            origem.des_dispositivo,
            origem.idc_posicao_tela,
            origem.idc_sensor_dsp,
            origem.nro_serie,
            origem.idc_status,
            origem.ip_dispositivo
        ))

        cr_sql.execute("""select ident_current('DispositivoAcesso')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('DispositivoAcesso',?,?)",(pkSql, linha_log.pk,))
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
            tabela,
            operacao,
            pk
        from mc_log
        where
            tabela = 'disp_entrada_saida'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
