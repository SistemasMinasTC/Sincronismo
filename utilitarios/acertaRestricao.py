#!/usr/bin/python
#

from conexoes import *
from collections import namedtuple

def main():
    with conecta_informix() as ifx:
        cr_ifx = ifx.cursor()
        
        with conecta_mssql() as sql:
            cr_sql = sql.cursor()
            cr_sql.execute("""
                select
                    case IdClube
                        when 'MTC' then 'minas'
                        when 'MTNC' then 'nautico'
                        when 'MSDR' then 'serra'
                    end as cod_clube,
                    NPF as cod_associado,
                    isnull(CodigoRestricao,0) as CodigoRestricao,
                    IdAssociado
                from Associado
                inner join Cota on Cota.IdCota = Associado.IdCota
                where
                    Associado.DataExclusao is null and
                    Associado.NPF <> 2184 -- cotas do Minas
                order by 1,2
            """)
            Linha = namedtuple('Linha', [col[0] for col in cr_sql.description])
            
            for linha in (Linha(*l) for l in cr_sql):
                cr_ifx.execute(f'select {linha.cod_clube}:restricao({linha.cod_associado}, today) from dual')
                CodigoRestricao = cr_ifx.fetchone()[0]
            
                print(linha,  CodigoRestricao, end=" ")
                
                if CodigoRestricao and int(CodigoRestricao) != int(linha.CodigoRestricao):
                    with conecta_mssql() as update:
                        cr_update = update.cursor()
                        cr_update.execute(f"""
                            update Associado
                            set
                                CodigoRestricao = {CodigoRestricao}
                            where
                                IdAssociado = {linha.IdAssociado} 
                        """)
                        print('alterado')
                else:
                    print()


# Teste
#
if __name__ == "__main__":
    main()
