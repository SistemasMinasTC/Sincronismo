#!/usr/bin/python
#

from recordtype import recordtype
import os

def convert(conn_ifx, conn_sql, linha_log):
    cr_ifx = conn_ifx.cursor()
    cr_ifx.execute('execute procedure em_sincronismo()')

    Chave = recordtype('Chave', 'cod_dias_let, cod_curso, cod_turma')
    chave = Chave(*linha_log.pk.split('|'))

    cr_ifx.execute(f"""
        insert into mc_log
        (
            data_hora,
            banco,
            tabela,
            operacao,
            pk
        ) values (current,'{linha_log.banco}','turma','upd','{chave.cod_curso}|{chave.cod_turma}')
    """)



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
            tabela = 'dias_let_curso'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        convert(ifx, sql, linha)
