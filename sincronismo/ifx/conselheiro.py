#!/usr/bin/python
#

import sys
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
 
    linha_log.pk = cod_clube + '|' + linha_log.pk
 
    Chave = recordtype('Chave', 'cod_clube, cod_associado')
    chave = Chave(*linha_log.pk.split('|'))
 
    if linha_log.operacao == 'del':
        # nulifica quem aponta para este conselheiro como suplente antes de excluir
        #
        cr_sql.execute("""
            update Conselheiro set
                IdConselheiroSuplente = null
            where IdConselheiroSuplente = (
                select top 1 Conselheiro.IdConselheiro
                from Conselheiro
                inner join Associado on Associado.IdAssociado = Conselheiro.IdAssociado
                inner join Cota on Cota.IdCota = Associado.IdCota
                where
                    Cota.IdClube = ? and
                    Associado.NPF = ?
            )
        """, (
            chave.cod_clube,
            chave.cod_associado,
        ))
 
        cr_sql.execute("""
            delete from Conselheiro
            where IdConselheiro = (
                select top 1 Conselheiro.IdConselheiro
                from Conselheiro
                inner join Associado on Associado.IdAssociado = Conselheiro.IdAssociado
                inner join Cota on Cota.IdCota = Associado.IdCota
                where
                    Cota.IdClube = ? and
                    Associado.NPF = ?
            )
        """, (
            chave.cod_clube,
            chave.cod_associado,
        ))
 
        cr_sql.close()
        return
 
    cr_ifx.execute(f"""
        select
            cod_associado,
            case trim(idt_tipo_conselho)
                when 'S' then 'Suplente'
                when 'N' then 'Nato'
                when 'E' then 'Eleito'
                else trim(idt_tipo_conselho)
            end as idt_tipo_conselho,
            case trim(idt_status)
                when 'A' then 'Ativo'
                when 'L' then 'Licenca'
                when 'I' then 'Inativo'
                else trim(idt_status)
            end as idt_status,
            nro_suplente,
            trim(cod_eleicao) as cod_eleicao,
            dat_posse,
            cod_cons_subst,
            trim(cod_funcao) as cod_funcao
        from {linha_log.banco}:conselheiro as conselheiro
        where
            cod_associado = ?
    """, (
        chave.cod_associado,
    ))
 
    Linha = recordtype('Linha', [col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None
 
    if not origem:
        raise Exception('Conselheiro não encontrado no Informix')
 

    cr_sql.execute("""
        update Conselheiro set
            TipoConselheiro = ?,
            Status = ?,
            NumeroSuplente = ?,
            CodigoEleicao = ?,
            DataPosse = ?,
            IdConselheiroSuplente = (
                select top 1 Conselheiro.IdConselheiro
                from Conselheiro
                inner join Associado on Associado.IdAssociado = Conselheiro.IdAssociado
                inner join Cota on Cota.IdCota = Associado.IdCota
                where
                    Cota.IdClube = ? and
                    Associado.NPF = ?
            ),
            CodigoFuncao = ?,
            IdClube = ?
        where IdAssociado = (
            select top 1 Associado.IdAssociado
            from Associado
            inner join Cota on Cota.IdCota = Associado.IdCota
            where
                Cota.IdClube = ? and
                Associado.NPF = ?
        )
    """, (
        origem.idt_tipo_conselho,
        origem.idt_status,
        origem.nro_suplente,
        origem.cod_eleicao,
        origem.dat_posse,
        chave.cod_clube, 
        origem.cod_cons_subst,
        origem.cod_funcao,
        chave.cod_clube,
        chave.cod_clube, 
        origem.cod_associado,
    ))
 
    if cr_sql.rowcount == 0:
        cr_sql.execute("""
            insert into Conselheiro
            (
                IdAssociado,
                TipoConselheiro,
                Status,
                NumeroSuplente,
                CodigoEleicao,
                DataPosse,
                IdConselheiroSuplente,
                CodigoFuncao,
                IdClube
            ) values (
                (
                    select top 1 Associado.IdAssociado
                    from Associado
                    inner join Cota on Cota.IdCota = Associado.IdCota
                    where
                        Cota.IdClube = ? and
                        Associado.NPF = ?
                ),
                ?, ?, ?, ?, ?, 
                (
                    select top 1 Conselheiro.IdConselheiro
                    from Conselheiro
                    inner join Associado on Associado.IdAssociado = Conselheiro.IdAssociado
                    inner join Cota on Cota.IdCota = Associado.IdCota
                    where
                        Cota.IdClube = ? and
                        Associado.NPF = ?
                ),
                ?,
                ?
            )
        """, (
            chave.cod_clube, 
            origem.cod_associado,
            origem.idt_tipo_conselho,
            origem.idt_status,
            origem.nro_suplente,
            origem.cod_eleicao,
            origem.dat_posse,
            chave.cod_clube, 
            origem.cod_cons_subst,
            origem.cod_funcao,
            chave.cod_clube,
        ))
 
    cr_sql.close()
 
 
#Teste
#
if __name__ == "__main__":
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
            tabela = 'conselheiro'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
