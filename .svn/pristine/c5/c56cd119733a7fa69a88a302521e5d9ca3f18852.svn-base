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

    linha_log.pk = cod_clube + '|' + linha_log.pk

    Chave = recordtype('Chave', 'cod_clube, cod_tipo_associado, cod_cota')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            update Cota
            set
                DataEncerramento = getdate()
            where
                IdCota = (select PkSql from PkDePara where Tabela = 'Cota' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            _cota_.cod_tipo_associado,
             _cota_.cod_cota,
            '{cod_clube}|' ||  _cota_.cod_tipo_associado || '|' ||  _cota_.cod_tipo_cota as cod_tipo_cota,
             _cota_.dat_criacao_cota,
            case  _cota_.idt_status
               when 'N' then 'Normal'
               when 'I' then 'Isenta'
               when 'A' then 'Adiantado'
               when 'D' then 'Devedora'
            end as idt_status,
            _cota_.dat_prev_encerra,
            (select des_email from {linha_log.banco}:associado as associado where associado.cod_associado = _cota_.cod_associado_tit) as des_email,
            '{cod_clube}|' || cota_isenta.cod_tipo_isencao as cod_tipo_isencao,
            cota_isenta.dat_inicio_isencao,
            cota_isenta.dat_fim_isencao
        from {linha_log.banco}:_cota_ as _cota_
        left join {linha_log.banco}:cota_isenta as cota_isenta on
            cota_isenta.cod_tipo_associado = _cota_.cod_tipo_associado and
            cota_isenta.cod_cota = _cota_.cod_cota
        where
            _cota_.cod_tipo_associado = ? and
            _cota_.cod_cota = ?
    """,(
        chave.cod_tipo_associado,
        chave.cod_cota,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    origem = Linha(*linha) if (linha := cr_ifx.fetchone()) else None

    cr_sql.execute("""
        update Cota set
            IdClube = ?,
            TipoCota = ?,
            NumeroCota = ?,
            IdClasseCota = (select PkSql from PkDePara where Tabela = 'ClasseCota' and PkIfx = ?),
            DataCriacao = ?,
            StatusCota = ?,
            DataEncerramento = ?,
            EmailCobranca = ?,
            IdTipoIsencao = (select PkSql from PkDePara where Tabela = 'TipoIsencao' and PkIfx = ?),
            DataInicioIsencao = ?,
            DataFimIsencao = ?,
            UltimaAlteracao = getdate()
        where
            IdCota = (select PkSql from PkDePara where Tabela = 'Cota' and PkIfx = ?)
    """,(
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.cod_cota,
            origem.cod_tipo_cota,
            origem.dat_criacao_cota,
            origem.idt_status,
            origem.dat_prev_encerra,
            origem.des_email,
            origem.cod_tipo_isencao,
            origem.dat_inicio_isencao,
            origem.dat_fim_isencao,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into Cota
            (
                IdClube,
                TipoCota,
                NumeroCota,
                IdClasseCota,
                DataCriacao,
                StatusCota,
                DataEncerramento,
                EmailCobranca,
                IdTipoIsencao,
                DataInicioIsencao,
                DataFimIsencao
            ) values (
                ? /*IdClube*/,
                ? /*TipoCota*/,
                ? /*NumeroCota*/,
                (select PkSql from PkDePara where Tabela = 'ClasseCota' and PkIfx = ?) /*IdClasseCota*/,
                ? /*DataCriacao*/,
                ? /*StatusCota*/,
                ? /*DataEncerramento*/,
                ? /*EmailCobranca*/,
                (select PkSql from PkDePara where Tabela = 'TipoIsencao' and PkIfx = ?) /*IdTipoIsencao*/,
                ? /*DataInicioIsencao*/,
                ? /*DataFimIsencao*/
            )
        """,(
            origem.cod_clube,
            origem.cod_tipo_associado,
            origem.cod_cota,
            origem.cod_tipo_cota,
            origem.dat_criacao_cota,
            origem.idt_status,
            origem.dat_prev_encerra,
            origem.des_email,
            origem.cod_tipo_isencao,
            origem.dat_inicio_isencao,
            origem.dat_fim_isencao,
        ))

        cr_sql.execute("""select ident_current('Cota')""")
        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('Cota',?,?)",(pkSql, linha_log.pk,))
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
            tabela = '_cota_'
        order by data_hora
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
