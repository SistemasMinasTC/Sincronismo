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

    Chave = recordtype('Chave', 'cod_clube, idf_tipo_servico')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from ServicoTipo
            where
                IdServicoTipo = (select PkSql from PkDePara where Tabela = 'ServicoTipo' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}' as cod_clube,
            des_tipo_servico,
            cod_receita,
            idt_tipo_servico,
            nro_dias,
            case idt_tipo_cobranca
                when 'D' then 'Diario'
                when 'M' then 'Mensal'
                else idt_tipo_cobranca
            end as idt_tipo_cobranca,
            vlr_servico,
            nro_vagas,
            des_cupom,
            des_cupom_cancelamento,
            nvl(idt_ativo,'S') = 'S' as idt_ativo,
            cod_usuario,
            dat_ultima_intervencao,
            idt_area,
            idc_isenta_diretor = 'S' as idc_isenta_diretor,
            idt_devolve_proporcional
        from tipo_servico
        where
            idf_tipo_servico = ?
    """,(
        chave.idf_tipo_servico,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""
        update ServicoTipo set
            IdClube = ?,
            NomeServicoTipo = ?,
            IdReceita = (select IdReceita from Receita where Receita.IdClube = ? and CodigoReceita = ?),
            PeriodoUso = ?,
            NumeroDias = ?,
            TipoCobranca = ?,
            Valor = ?,
            Vagas = ?,
            TextoCupomAdesao = ?,
            TextoCupomCancelamento = ?,
            Ativo = ?,
            IdUsuario = ?,
            UltimaAlteracao = ?,
            IdArea = ?,
            IsentaDiretor = ?,
            DevolveProporcional = ?,
            UltimaAlteracao = getdate()
        where
            IdServicoTipo = (select PkSql from PkDePara where Tabela = 'ServicoTipo' and PkIfx = ?)
    """,(
            origem.cod_clube,
            origem.des_tipo_servico,
            origem.cod_clube,
            origem.cod_receita,
            origem.idt_tipo_servico,
            origem.nro_dias,
            origem.idt_tipo_cobranca,
            origem.vlr_servico,
            origem.nro_vagas,
            origem.des_cupom,
            origem.des_cupom_cancelamento,
            origem.idt_ativo,
            origem.cod_usuario,
            origem.dat_ultima_intervencao,
            origem.idt_area,
            origem.idc_isenta_diretor,
            origem.idt_devolve_proporcional,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into ServicoTipo
            (
                IdClube,
                NomeServicoTipo,
                IdReceita,
                PeriodoUso,
                NumeroDias,
                TipoCobranca,
                Valor,
                Vagas,
                TextoCupomAdesao,
                TextoCupomCancelamento,
                Ativo,
                IdUsuario,
                UltimaAlteracao,
                IdArea,
                IsentaDiretor,
                DevolveProporcional
            ) output inserted.IdServicoTipo values (
                ? /*IdClube*/,
                ? /*NomeServicoTipo*/,
                (select PkSql from PkDePara where Tabela = 'Receita' and PkIfx = ?) /*IdReceita*/,
                ? /*PeriodoUso*/,
                ? /*NumeroDias*/,
                ? /*TipoCobranca*/,
                ? /*Valor*/,
                ? /*Vagas*/,
                ? /*TextoCupomAdesao*/,
                ? /*TextoCupomCancelamento*/,
                ? /*Ativo*/,
                ? /*IdUsuario*/,
                ? /*UltimaAlteracao*/,
                ? /*IdArea*/,
                ? /*IsentaDiretor*/,
                ? /*DevolveProporcional*/
            )
        """,(
            origem.cod_clube,
            origem.des_tipo_servico,
            origem.cod_receita,
            origem.idt_tipo_servico,
            origem.nro_dias,
            origem.idt_tipo_cobranca,
            origem.vlr_servico,
            origem.nro_vagas,
            origem.des_cupom,
            origem.des_cupom_cancelamento,
            origem.idt_ativo,
            origem.cod_usuario,
            origem.dat_ultima_intervencao,
            origem.idt_area,
            origem.idc_isenta_diretor,
            origem.idt_devolve_proporcional
        ))

        pkSql = cr_sql.fetchval()

        cr_sql.execute("insert into PkDePara values ('ServicoTipo',?,?)",(pkSql, linha_log.pk,))
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
            tabela = 'tipo_servico'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
