#!/usr/bin/python
#
from pywebio.output import *
from biblioteca.conexoes import *
def createSincronismoSqlIfx(tabelaIfx: str, primaryKeyIfx: list, tabelaSql: str, primaryKeySql: list, autoIncremento: bool, dePara: dict):
    if autoIncremento:
        projetaPkIfx = f""",
            PkDePara.PkIfx"""
    else:
        projetaPkIfx = f"""
            concat({",'|'".join([dePara[col][0] for col in primaryKeyIfx])},null) as PkIfx"""

    leftJoinPkDePara = f"""
        left join PkDePara on
            Tabela = '{tabelaSql}' and
            PkDePara.PkSql = Id{tabelaSql}"""


    colunasIfx = list(dePara.keys())
    colunasIfxUpdate = [dP for dP in dePara if dP not in primaryKeyIfx]

    colunasSql = [v[0] for v in dePara.values()]
    colunasSqlUpdate = [dePara[dP][0] for dP in colunasIfxUpdate]

    NL='\n'

    arquivo = open(f'sincronismo/{tabelaSql}.py','w')

    arquivo.write(f'''\
#!/usr/bin/python
#

import json
from recordtype import recordtype

def convert(conn_ifx, conn_sql, linha_log):
    cr_sql = conn_sql.cursor()
    try:
        cr_sql.execute('create table #sincronizando (dummy char(1))')
    except:
        pass

    cr_ifx = conn_ifx.cursor()
    cr_ifx.execute('execute procedure em_sincronismo()')



    if linha_log.operacao == 'del':
        excluido = json.loads(linha_log.excluido)[0]
        Linha = recordtype('Linha',excluido.keys())
        origem = Linha(**excluido)

        cr_sql.execute(f"""
            select
                case
                    when IdClube = 'MTC' then 'minas'
                    else 'nautico'
                end as IdBanco
            from Cota
            where IdCota = ?
        """,(
            origem.IdCota,
        ))

        linha_log.banco = cr_sql.fetchval()

        cr_ifx.execute(f"""
            delete from {{linha_log.banco}}:{tabelaIfx}
            where
                nro_fatura = ?
        """, (
            origem.NumeroFatura,
        ))

        cr_sql.close()

        return

    cr_sql.execute("""
        select
            {f',{NL}            '.join(colunasSql)}{projetaPkIfx}
        from {tabelaSql}{leftJoinPkDePara if autoIncremento else ''}
        where
            Id{tabelaSql} = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.close()

    Chave = recordtype('Chave', '{'cod_clube, ' if 'cod_clube' in colunasIfx else ''}{','.join(primaryKeyIfx,)}')
    chave = Chave(*origem.PkIfx.split('|'))

    linha_log.banco = 'minas' if 'cod_clube' not in chave._fields else 'nautico' if chave.cod_clube == 'MTNC' else 'minas'

    cr_ifx.execute(f"""
        update {{linha_log.banco}}:{tabelaIfx} set
            {f',{NL}            '.join(col+' = ?' for col in colunasIfxUpdate)}
        where
            {{f' = ? and{NL}            '.join(primaryKeyIfx,)}} = ?
    """,(
            origem.{f',{NL}            origem.'.join(colunasSqlUpdate)},
            chave.{f',{NL}        chave.'.join(primaryKeyIfx,)},
    ))

    if cr_ifx.rowcount == 0:
        cr_ifx.execute(f"""
            insert into {{linha_log.banco}}:{tabelaIfx}
            (
                {f',{NL}                '.join(colunasIfx,)}
            ) values (
                {f',{NL}                '.join('? {'+col+'}' for col in colunasIfx)}
            )
        """,(
            origem.{f',{NL}            origem.'.join(colunasSql,)}
        ))

    cr_ifx.close()

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

    cr_sql = sql.cursor()

    cr_sql.execute("""
        select
            id,
            data_hora,
            banco,
            tabela,
            operacao,
            pk
        from mc_log
        where
            tabela = '{tabelaSql}'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)

''')

    arquivo.close()

# Teste
#
if __name__ == "__main__":
    createSincronismoSqlIfx('nacionalidade',('cod_nacionalidade',),'Nacionalidade',('IdNacionalidade',),False,{
        'cod_nacionalidade': ('IdNacionalidade','None'),
        'nom_pais': ('Pais','None'),
        'nom_nacionalidade': ('Nome',None)
    })
