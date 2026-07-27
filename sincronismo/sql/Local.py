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

    cr_sql.execute("""
        select
            IdLocal,
            IdUnidade,
            NomeLocal,
            GrupoLocais,
            IdLocalSuperior,
            Superior.nro_seq_local as nro_seq_superior,
            Ativo,
            PermiteReserva,
            TemLuminaria,
            CapacidadeMaxima,
            IdUsuario,
            UltimaAlteracao,
            Area,
            Descricao,
            CodigoCentroCusto,
            ValorHoraExtra,
            ValorEntrada,
            case NomeLocalTipo
                when 'OUTROS' then 'O'
                when 'PISCINA' then 'P'
                when 'QUADRA' then 'Q'
                when 'SALA' then'S'
            end as idt_local,
            IdReceitaLocacao,
            unidade.PkSql as cod_unidade
        from Local
        inner join LocalTipo on LocalTipo.IdLocalTipo = Local.IdLocalTipo
        left join PkDePara as Unidade on Unidade.PkSql = Local.IdUnidade
        left join Local as Superior on Superior.IdLocal = Local.IdLocalSuperior
        where
            IdLocal = ?
    """,(
        linha_log.pk,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])
    linha = cr_sql.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.close()

    if not origem:
        return

    linha_log.banco = 'minas' if 'cod_clube' not in chave._fields else 'nautico' if chave.cod_clube == 'MTNC' else 'minas'
    
    cod_unidade = origem.cod_unidade.split('|')[1]

    if linha_log.operacao == 'del':
        cr_ifx.execute(f"""
            delete from {linha_log.banco}:local
            where
                nro_seq_local = ?
        """, (
            cod_unidade,
            origem.nro_seq_local,
        ))

        return

    cr_ifx.execute(f"""
        update {linha_log.banco}:local set
            nom_local,
            idt_local,
            idc_grupo,
            cod_uni_subord,
            nro_seq_loc_subord,
            idc_ativo,
            idc_reserva,
            idc_luminaria,
            capacidade_maxima,
            cod_centro_responsabilidade
        where
            cod_unidade = ? and
            nro_seq_local = ?
    """,(
            origem.NomeLocal,
            origem.idt_local,
            'S' if origem.GrupoLocais else 'N',
            cod_unidade,  
            origem.nro_seq_superior,
            'S' if origem.Ativo else 'N',
            'S' if origem.PermiteReserva else 'N',
            'S' if origem.TemLuminaria else 'N',
            CapacidadeMaxima,
            CodigoCentroCusto,
            cod_unidade, 
            origem.nro_seq_local
    ))

    if cr_ifx.rowcount == 0:
        cr_ifx.execute(f"""
            insert into {linha_log.banco}:Local
            (
                cod_unidade,
                nro_seq_local,
                nom_local,
                idt_local,
                idc_grupo,
                cod_uni_subord,
                nro_seq_loc_subord,
                idc_ativo,
                idc_reserva,
                idc_luminaria,
                capacidade_maxima,
                cod_centro_responsabilidade
            ) values (
                ? {{cod_unidade}},
                ? {{nro_seq_local}},
                ? {{nom_local}},
                ? {{idt_local}},
                ? {{idc_grupo}},
                ? {{cod_uni_subord}},
                ? {{nro_seq_loc_subord}},
                ? {{idc_ativo}},
                ? {{idc_reserva}},
                ? {{idc_luminaria}},
                ? {{capacidade_maxima}},
                ? {{cod_centro_responsabilidade}}
            )
        """,(
            cod_unidade,
            origem.nro_seq_local, 
            origem.NomeLocal,
            origem.idt_local,
            'S' if origem.GrupoLocais else 'N',
            cod_unidade,  
            origem.nro_seq_superior,
            'S' if origem.Ativo else 'N',
            'S' if origem.PermiteReserva else 'N',
            'S' if origem.TemLuminaria else 'N',
            CapacidadeMaxima,
            CodigoCentroCusto,
        ))

    cr_ifx.close()

# Teste
#
if __name__ == "__main__":
    import sys
    from conexoes import *

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
            tabela = 'Local'
    """)

    Linha = recordtype('Linha',[col[0] for col in cr_sql.description])

    for linha in [Linha(*l) for l in cr_sql]:
        print(linha,end=' ')
        try:
            convert(ifx, sql, linha)
            print('ok')
        except Exception as erro:
            print(erro)


