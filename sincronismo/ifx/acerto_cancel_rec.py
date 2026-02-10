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

    Chave = recordtype('Chave', 'cod_clube, nro_acerto_cancel, cod_tipo_associado, cod_cota, cod_associado, cod_receita, dat_receita, hor_receita')
    chave = Chave(*linha_log.pk.split('|'))

    cr_ifx.execute(f"""
        select
            acerto_cancel.dat_acerto_cancel,
            acerto_cancel.cod_motivo,
            acerto_cancel.des_observacao,
            acerto_cancel.mla_funcionario,
            to_char(acerto_cancel_rec.dat_receita, '%Y-%m-%d ') || hor_receita as dat_receita
        from {linha_log.banco}:acerto_cancel_rec as acerto_cancel_rec
        inner join {linha_log.banco}:acerto_cancel as acerto_cancel on acerto_cancel.nro_acerto_cancel = acerto_cancel_rec.nro_acerto_cancel
        where
            acerto_cancel_rec.nro_acerto_cancel = ? and
            acerto_cancel_rec.cod_tipo_associado = ? and
            acerto_cancel_rec.cod_cota = ? and
            acerto_cancel_rec.cod_associado = ? and
            acerto_cancel_rec.cod_receita = ? and
            acerto_cancel_rec.dat_receita = '{chave.dat_receita}'::date and
            acerto_cancel_rec.hor_receita = ?
    """,(
        chave.nro_acerto_cancel,
        chave.cod_tipo_associado,
        chave.cod_cota,
        chave.cod_associado,
        chave.cod_receita,
        chave.hor_receita,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None


    cr_sql.execute("""
        update ReceitaCota set
            IdAcerto = (select PkSql from PkDePara where Tabela = 'Acerto' and PkIfx = ?),
            DataCancelamento = ?,
            IdMotivoCancelamento = (select PkSql from PkDePara where Tabela = 'Motivo' and PkIfx = ?),
            Observacao = ?,
            IdUsuarioCancelamento = (select min(IdUsuario) from Usuario where Matricula = ?),
            UltimaAlteracao = getdate()
        where
            IdAssociado =
            (
                select IdAssociado from Associado
                where
                    IdPessoa = (select PkSql from PkDePara where PkIfx = ?) and
                    IdCota = (select IdCota from Cota where IdClube = ? and NumeroCota = ?)
            ) and
            IdReceita = (select IdReceita from Receita where IdClube = ? and CodigoReceita = ?) and
            DataReceita = ?
    """,(

        f'{chave.cod_clube}|{chave.nro_acerto_cancel}',
        origem.dat_acerto_cancel,
        origem.cod_motivo,
        origem.des_observacao,
        origem.mla_funcionario,

        f'Associado|{chave.cod_associado}',
        chave.cod_clube,
        chave.cod_cota,
        chave.cod_clube,
        chave.cod_receita,
        origem.dat_receita
    ))

    if cr_sql.rowcount == 0:
        raise Exception('Receita não encontrada')

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
            tabela = 'acerto_cancel_rec'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
