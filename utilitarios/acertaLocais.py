#!/usr/bin/python
#

from conexoes import *
from collections import namedtuple

def main():
    with conecta_informix() as ifx:
        cr_ifx = ifx.cursor()
        with conecta_mssql() as sql_update:
            cr_sql_update = sql_update.cursor()
            with conecta_mssql() as sql:
                cr_sql = sql.cursor()
                cr_sql.execute("""
                    select
                        IdLocal,
                        IdUnidade,
                        substring(NomeLocal,1,30) as NomeLocal,
                        Ativo,
                        case 
                            when PkIfx like 'MTC%' then 'minas'
                            when PkIfx like 'MTNC%' then 'nautico'
                            when PkIfx like 'MSDR%' then 'serra'
                        end as banco
                    from tmp_local
                    inner join PkDePara on
                        Tabela = 'Unidade' and
                        PkSql = IdUnidade
                    where
                        nro_seq_local is null
                """)
                Linha = namedtuple('Linha', [col[0] for col in cr_sql.description])
                
                for linha in (Linha(*l) for l in cr_sql):
                    print (linha.IdLocal, linha.NomeLocal, end=' ')
                    cr_ifx.execute(f"""
                        select nro_seq_local
                        from {linha.banco}:tmp_local
                        where
                            nom_local = ? and
                            idc_ativo = ? 
                    """,(
                        linha.NomeLocal.strip(), 
                        'S' if linha.Ativo == 1 else 'N', 
                    ))
                    
                    local = cr_ifx.fetchone()
                    
                    if local:
                        cr_sql_update.execute("""
                            update tmp_local
                            set
                                nro_seq_local = ?
                            where
                                IdLocal = ?
                        """, (
                            local[0], 
                            linha.IdLocal, 
                        ))
                    print
                else:
                    print('não encontrado')


# Execução
#
if __name__ == "__main__":
    main()
