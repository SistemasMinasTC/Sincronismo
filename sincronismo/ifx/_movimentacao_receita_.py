#!/usr/bin/python
#

import time
from recordtype import recordtype

def _busca_dados_complementares(cr_sql,chave):
    cr_sql.execute(f"""
        select
            Associado.IdAssociado,
            Receita.IdReceita,
            ReceitaCota.IdReceitaCota
        from Clube
        inner join Cota on
            Cota.IdClube = Clube.IdClube and
            Cota.NumeroCota = ?
        inner join Associado on
            Associado.IdCota = Cota.IdCota and
            Associado.NPF = ?
        inner join Receita on
            Receita.IdClube = Clube.IdClube and
            Receita.CodigoReceita = ?
        left join ReceitaCota on
            ReceitaCota.IdAssociado = Associado.IdAssociado and
            ReceitaCota.IdReceita = Receita.IdReceita and
            ReceitaCota.DataReceita = ?
        where
            Clube.IdClube = ?
    """,(
        chave.cod_cota,
        chave.cod_associado,
        chave.cod_receita,
        f"{(d := chave.dat_receita.split('/'))[2]}-{d[1]}-{d[0]} {chave.hor_receita}",
        chave.cod_clube
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    return Linha(*linha) if (linha := cr_sql.fetchone()) else None


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

    Chave = recordtype('Chave', 'cod_clube, cod_tipo_associado, cod_cota, cod_associado, cod_receita, dat_receita, hor_receita')
    chave = Chave(*linha_log.pk.split('|'))

    DataReceita = f"{(d := chave.dat_receita.split('/'))[2]}-{d[1]}-{d[0]} {chave.hor_receita}"

    if linha_log.operacao == 'del':
        dados =_busca_dados_complementares(cr_sql,chave)

        if not dados:
            cr_sql.close()
            raise Exception('Associado ou Receita não encontrados')
            return

        cr_ifx.execute(f"""
            select count(*) as quantidade
            from {linha_log.banco}:_movimentacao_receita_ as _movimentacao_receita_
            where
                cod_tipo_associado = ? and
                cod_cota = ? and
                cod_associado = ? and
                cod_receita = ? and
                to_char(dat_receita,'%d/%m/%Y') = ? and
                hor_receita = ?
        """,(
            chave.cod_tipo_associado,
            chave.cod_cota,
            chave.cod_associado,
            chave.cod_receita,
            chave.dat_receita,
            chave.hor_receita,
        ))

        quantidade = cr_ifx.fetchone()[0]

        if not quantidade:
            cr_sql.execute(f"""
                update ReceitaCota
                set Status = 'Excluida'
                where IdReceitaCota = ?
            """, (
                dados.IdReceitaCota,
            ))

        cr_sql.close()
        cr_ifx.close()

        return

    cr_ifx.execute(f"""
        select
            '{cod_clube}|' || cod_plano as cod_plano,
            nro_parcela,
            case
                when _movimentacao_receita_.cod_receita in (1, 2, 12) then year(dat_vencimento)
                when nro_parcela between 1 and 12 and (cod_receita in
                    (
                        select turma.cod_receita from {linha_log.banco}:turma as turma
                        inner join {linha_log.banco}:curso as curso on
                            curso.cod_curso = turma.cod_curso and
                            idc_curso_temp = 'N'
                    ) or cod_receita = 8) then
                    case
                        when (year(dat_vencimento) * 12 + month(dat_vencimento)) - (year(dat_receita) * 12 + month(dat_receita)) > 2 then year(dat_vencimento)
                        when nro_parcela = month(dat_receita) then year(dat_receita)
                        when month(dat_receita) >= 10 and nro_parcela <= 3 then year(dat_receita)+1
                        when month(dat_receita) <= 3 and nro_parcela >= 10 then year(dat_receita)-1
                        else year(dat_receita)
                    end
                else year(dat_receita)
            end as ano_parcela,
            dat_vencimento,
            vlr_receita,
            vlr_desconto,
            dat_pagto,
            vlr_pago,
            dat_gravacao,
            trim
            (
                case
                    when nom_view in ('receita_assoc_excl','posicao_assoc_excl') then 'Excluida'
                    when idt_status = 'B' then 'Paga'
                    when idt_status = 'C' then 'Cancelada'
                    when idt_status = 'D' then 'Paga'
                    when idt_status = 'M' then 'Paga'
                    when idt_status = 'P' then 'Paga'
                    when idt_status = 'T' then 'Paga'
                    else 'Aberta'
                end
            ) as idt_status,
            idf_movimentacao
        from {linha_log.banco}:_movimentacao_receita_ as _movimentacao_receita_
        where
            cod_tipo_associado = ? and
            cod_cota = ? and
            cod_associado = ? and
            cod_receita = ? and
            to_char(dat_receita,'%d/%m/%Y') = ? and
            hor_receita = ?
    """,(
        chave.cod_tipo_associado,
        chave.cod_cota,
        chave.cod_associado,
        chave.cod_receita,
        chave.dat_receita,
        chave.hor_receita,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    origem = Linha(*linha) if (linha := cr_ifx.fetchone()) else None

    if not origem:
        cr_sql.close()
        return


    dados =_busca_dados_complementares(cr_sql,chave)

    if not dados:
        cr_sql.close()
        raise Exception('Associado ou Receita não encontrados')
        return

    cr_sql.execute("""
        update ReceitaCota set
            IdPlanoCobranca = (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?),
            NumeroDaParcela = ?,
            AnoDaParcela = ?,
            DataVencimento = ?,
            ValorReceita = ?,
            ValorDesconto = ?,
            DataPagto = ?,
            ValorPago = ?,
            DataInformacaoPagamento = ?,
            Status = ?,
            idf_movimentacao = ?,
            UltimaAlteracao = getdate()
        where
            IdReceitaCota = ?
    """,(
            origem.cod_plano,
            origem.nro_parcela,
            origem.ano_parcela,
            origem.dat_vencimento,
            origem.vlr_receita,
            origem.vlr_desconto,
            origem.dat_pagto,
            origem.vlr_pago,
            origem.dat_gravacao,
            origem.idt_status,
            origem.idf_movimentacao,
            dados.IdReceitaCota,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute(f"""
            insert into ReceitaCota
            (
                IdAssociado,
                IdReceita,
                DataReceita,
                IdPlanoCobranca,
                NumeroDaParcela,
                AnoDaParcela,
                DataVencimento,
                ValorReceita,
                ValorDesconto,
                DataPagto,
                ValorPago,
                DataInformacaoPagamento,
                Status,
                idf_movimentacao
            ) values (
                ? /*IdAssociado*/,
                ? /*IdReceita*/,
                ? /*DataReceita*/,
                (select PkSql from PkDePara where Tabela = 'PlanoCobranca' and PkIfx = ?) /*IdPlanoCobranca*/,
                ? /*NumeroDaParcela*/,
                ? /*AnoDaParcela*/,
                ? /*DataVencimento*/,
                ? /*ValorReceita*/,
                ? /*ValorDesconto*/,
                ? /*DataPagto*/,
                ? /*ValorPago*/,
                ? /*DataInformacaoPagamento*/,
                ? /*Status*/,
                ? /*idf_movimentacao*/
            )
        """,(
            dados.IdAssociado,
            dados.IdReceita,
            DataReceita,
            origem.cod_plano,
            origem.nro_parcela,
            origem.ano_parcela,
            origem.dat_vencimento,
            origem.vlr_receita,
            origem.vlr_desconto,
            origem.dat_pagto,
            origem.vlr_pago,
            origem.dat_gravacao,
            origem.idt_status,
            origem.idf_movimentacao
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
            tabela = '_movimentacao_receita_' and
            tentativas = 3
        order by data_hora
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha, end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            cr_sql = sql.cursor()
            print(erro)
