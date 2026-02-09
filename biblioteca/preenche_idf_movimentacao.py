#!/usr/bin/env python
# -*- coding:utf-8 -*-
#

import os, sys, pyodbc
from recordtype import recordtype

from conexoes import *

#------------------------------------------------------------------------------------------------------------
ifx = conecta_informix()
cr_ifx = ifx.cursor()
sql = conecta_mssql()
cr_sql = sql.cursor()
sql_update = conecta_mssql()
cr_update = sql_update.cursor()

cr_sql.execute('create table #sincronizando (dummy char(1))')

cr_sql.execute("""
    select
        IdReceitaCota,
        Cota.IdClube,
        Cota.TipoCota,
        Cota.NumeroCota,
        Associado.NPF,
        Receita.CodigoReceita,
        cast(DataReceita as varchar(30)) as DataReceita
    from ReceitaCota
    inner join Associado on Associado.IdAssociado = ReceitaCota.IdAssociado
    inner join Cota on Cota.IdCota = Associado.IdCota
    inner join Receita on Receita.IdReceita = ReceitaCota.IdReceita
    where
       DataReceita >= '2017-01-01' and
       idf_movimentacao is null
""")

Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

quantidade = 0

for linha in (Linha(*l) for l in cr_sql):
    quantidade += 1
    print(quantidade,linha.IdReceitaCota,linha.TipoCota,linha.NumeroCota,linha.DataReceita)
    Data,Hora = linha.DataReceita.split()

    cr_ifx.execute(f"""
        select idf_movimentacao
        from {'minas' if linha.IdClube == 'MTC' else 'nautico'}:_movimentacao_receita_
        where
           cod_tipo_associado = ? and
           cod_cota = ? and
           cod_associado = ? and
           cod_receita = ? and
           dat_receita = ? and
           hor_receita  = ?
    """, (
        linha.TipoCota,
        linha.NumeroCota,
        linha.NPF,
        linha.CodigoReceita,
        Data,
        Hora,
    ))

    movto = cr_ifx.fetchone()
    idf_movimentacao = movto[0] if movto else None

    if idf_movimentacao:
        cr_update.execute("""
            update ReceitaCota
            set
                idf_movimentacao = ?
            where
                IdReceitaCota = ?
        """,(
            idf_movimentacao,
            linha.IdReceitaCota,
        ))

ifx.close()
sql.close()
