#!/usr/bin/python
#

from conexoes import *
from collections import namedtuple
from datetime import datetime

def main():
    print('Iniciando em ',datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    with conecta_informix() as ifx:
        cr_ifx = ifx.cursor()
        with conecta_mssql() as sql:
            cr_sql = sql.cursor()
            cr_sql.execute("""
                select distinct
                    case IdClube
                        when 'MTC' then 'minas'
                        when 'MTNC' then 'nautico'
                        when 'MSDR' then 'serra'
                    end as banco,
                    TipoCota,
                    NumeroCota
                from Fatura
                inner join Cota on Cota.IdCota = Fatura.IdCota
                where
                    ( 
                           DataPagamento >= dateadd(day,-3,getdate()) or
                           DataCancelamento >= dateadd(day,-3,getdate())
                    ) 
                order by 1,2,3
            """)
            
            Linha = namedtuple('Linha', [col[0] for col in cr_sql.description])
            
            for linha in (Linha(*l) for l in cr_sql):
                cr_ifx.execute(f"""execute procedure {linha.banco}:status_cota(?, ?)""", 
                (
                    linha.TipoCota,  
                    linha.NumeroCota, 
                ))
                
         
        print('Atualizando diferentes no sqlserver em ',datetime.now().strftime("%Y-%m-%d %H:%M:%S"))       
        with conecta_mssql() as sql:
            cr_sql = sql.cursor()
            cr_sql.execute("""
                select distinct
                    IdAssociado,
                    case IdClube
                        when 'MTC' then 'minas'
                        when 'MTNC' then 'nautico'
                        when 'MSDR' then 'serra'
                    end as banco,
                    NPF,
                    CodigoRestricao
                from Fatura
                inner join Cota on Cota.IdCota = Fatura.IdCota
                inner join Associado on Associado.IdCota = Cota.IdCota
                where
                    DataPagamento >= dateadd(day,-1,getdate()) and
                    Associado.DataExclusao is null
                order by 1,2,3
            """)
            
            Linha = namedtuple('Linha', [col[0] for col in cr_sql.description])
            
            for linha in (Linha(*l) for l in cr_sql):
                cr_ifx.execute(f"""
                    select 
                        trim(cod_tipo_restricao) as cod_tipo_restricao 
                    from {linha.banco}:pessoa_fisica 
                    where 
                    idt_pessoa = 1 and cod_pessoa = ?
                """, (
                    linha.NPF, 
                ))
                
                cod_restricao = l[0] if (l:=cr_ifx.fetchone()) else None
                
                if cod_restricao and cod_restricao != linha.CodigoRestricao:
                    print (linha.banco, linha.NPF, cod_restricao,  linha.CodigoRestricao)
                    with conecta_mssql() as sql_update:
                        cr_update = sql_update.cursor()
                        cr_update.execute("""
                            update Associado
                            set CodigoRestricao = ?
                            where IdAssociado = ?
                        """, (
                            cod_restricao, 
                            linha.IdAssociado
                        ))

# Execução
#
if __name__ == "__main__":
    main()
