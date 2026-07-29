#!/usr/bin/python
#

from conexoes import *
from collections import namedtuple
import sys

def confere(BANCO, MES, cod_receita):   
    with conecta_mssql() as sql:
        cr_sql = sql.cursor() 
        with conecta_informix(BANCO) as ifx:
            cr_ifx = ifx.cursor()
            
            cr_ifx.execute(f"""
                select
                   _movimentacao_receita_.cod_tipo_associado,
                   _movimentacao_receita_.cod_cota,
                   _movimentacao_receita_.cod_associado,
                   _movimentacao_receita_.cod_receita,
                   to_char(_movimentacao_receita_.dat_receita,'%Y-%m-%d ') || _movimentacao_receita_.hor_receita as dat_receita,
                   vlr_receita, 
                   vlr_desconto, 
                   idf_movimentacao
                from _movimentacao_receita_
                inner join receita on
                    receita.cod_receita = _movimentacao_receita_.cod_receita
                where
                   dat_receita = '1/{MES}/2026' and
                   hor_receita between '00:00:00.000' and '00:00:00.999' and
                   receita.cod_receita = {cod_receita}
            """)
            
            Linha = namedtuple('Linha', [col[0] for col in cr_ifx.description])
        
            for linha in (Linha(*l) for l in cr_ifx):
                cr_sql.execute("""
                    select count(*)
                    from ReceitaCota
                    inner join Associado on Associado.IdAssociado = ReceitaCota.IdAssociado
                    inner join Cota on Cota.IdCota = Associado.IdCota
                    inner join Receita on Receita.IdReceita = ReceitaCota.IdReceita
                    where
                        TipoCota = ? and
                        NumeroCota = ? and
                        NPF = ? and
                        CodigoReceita = ? and
                        DataReceita = ? and
                        ValorReceita = ? and
                        ValorDesconto = ?
                """, (
                    linha.cod_tipo_associado, 
                    linha.cod_cota, 
                    linha.cod_associado, 
                    linha.cod_receita, 
                    linha.dat_receita, 
                    linha.vlr_receita, 
                    linha.vlr_desconto, 
                ))
                
                qtd = cr_sql.fetchval()
                
                if not qtd:
                    print('tratar: ', linha)
                    

def main(BANCO, MES):
    QTD=0
    
    dados = namedtuple('Dados',  'quantidade, vlr_receita, vlr_desconto')
    
    with conecta_informix(BANCO) as ifx:
        cr_ifx = ifx.cursor()
        
        cr_ifx.execute(f"""
            select 
               cod_receita,
               count(*) as quantidade,
               sum(_movimentacao_receita_.vlr_receita) as vlr_receita,
               sum(_movimentacao_receita_.vlr_desconto) as vlr_desconto
            from _movimentacao_receita_ 
            where
               dat_receita = '1/{MES}/2026' and
               hor_receita between '00:00:00.000' and '00:00:00.999'
            group by 1
            order by 2 
        """)
        
        Linha = namedtuple('Linha', [col[0] for col in cr_ifx.description])
        
        informix={}
        for linha in (Linha(*l) for l in cr_ifx):
            informix[linha.cod_receita] = dados(linha.quantidade,  linha.vlr_receita,  linha.vlr_desconto)
            
    with conecta_mssql() as sql:
        cr_sql = sql.cursor()
        cr_sql.execute(f"""
            select 
                CodigoReceita as cod_receita,
                count(*) as quantidade,
                sum(ReceitaCota.ValorReceita) as vlr_receita, 
                sum(ReceitaCota.ValorDesconto) as vlr_desconto
            from ReceitaCota
            inner join Receita on Receita.IdReceita = ReceitaCota.IdReceita
            where
               IdClube = '{"MTC" if BANCO == "minas" else "MTNC"}' and
               ReceitaCota.DataReceita between '2026-{MES}-01 00:00:00.000' and  '2026-{MES}-01 00:00:00.999'
            group by CodigoReceita
        """)
        
        Linha = namedtuple('Linha', [col[0] for col in cr_sql.description])
        
        mssql={}
        
        for linha in (Linha(*l) for l in cr_sql):
            mssql[linha.cod_receita] = dados(linha.quantidade,  linha.vlr_receita,  linha.vlr_desconto)

    for cod_receita in informix:
        if QTD % 10 == 0:
            print(f'{QTD}/{len(informix)}')  
            
        if informix.get(cod_receita) != mssql.get(cod_receita):
            if QTD % 10:
                print(f'{QTD}/{len(informix)}') 
                
            print(f'Conferindo receita {cod_receita}')
            confere(BANCO, MES, cod_receita)
            
        QTD += 1

    if QTD % 10:
        print(f'{QTD}/{len(informix)}')  
# 
#
if __name__ == "__main__":
    BANCO = sys.argv[2]
    MES = sys.argv[3]
    
    main(BANCO, MES)
