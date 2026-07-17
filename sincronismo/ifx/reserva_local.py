#!/usr/bin/python
#

from recordtype import recordtype
import os
from datetime import datetime

def junta_data_hora(data, hora):
   if data is None:
      return None

   if hora is None:
      return datetime.strptime(
         data.strftime("%d/%m/%Y"),
         "%d/%m/%Y"
      )

   return datetime.strptime(
      data.strftime("%d/%m/%Y") + " " + hora.strftime("%H:%M:%S"),
      "%d/%m/%Y %H:%M:%S"
   )

def convert(conn_ifx, conn_sql, linha_log):
   cr_sql = conn_sql.cursor()
   try:
      cr_sql.execute('create table #sincronizando (dummy char(1))')
   except:
      pass

   cr_ifx = conn_ifx.cursor()
   cr_ifx.execute('execute procedure em_sincronismo()')

   pk = linha_log.pk

   linha_log.pk = 'reserva_local|'+linha_log.pk

   Chave = recordtype('Chave', 'tabela, nro_seq_reserva')
   chave = Chave(*linha_log.pk.split('|'))

   cr_ifx.execute(f"""
      insert into mc_log
      (
         data_hora,
         banco,
         tabela,
         operacao,
         pk
      )
      select
         current,
         '{linha_log.banco}',
         'reserva_local',
         'upd',
         nro_seq_reserva
      from {linha_log.banco}:reserva_local as reserva_local
      where
         nro_seq_reserva = {chave.nro_seq_reserva}
    """)

   if linha_log.operacao == 'del':
      cr_sql.close()
      return

   cr_ifx.execute(f"""
   select
      reserva_local.nro_seq_reserva,
      reserva_local.cod_unidade,
      case
         when reserva_local.cod_unidade = 1 then 'MINAS 1'
         when reserva_local.cod_unidade = 2 then 'MINAS 2'
         when reserva_local.cod_unidade = 3 then 'MINAS NAUTICO'
         when reserva_local.cod_unidade = 4 then 'MINAS COUNTRY'
      end as Unidade,
      reserva_local.nro_seq_local,
      tmp_local.nom_local,
      reserva_local.dat_inic_evento,
      reserva_local.dat_fim_evento,
      reserva_local.hor_inic_evento,
      reserva_local.hor_fim_evento,
      reserva_local.dat_mobilizacao,
      reserva_local.dat_desmobilizacao,
      reserva_local.hor_mobilizacao,
      reserva_local.hor_desmobilizacao,
      case
         when reserva_local.idc_aceita_comp = 'S' then 1
         else 0
      end as idc_aceita_comp,
      reserva_local.idc_cancelado,
      reserva_local.des_cancelamento,
      reserva_local.dat_cancelamento,
      reserva_local.hor_cancelamento,
      evento.cod_centro_custo,
      evento.mla_func_resp_exec,
      evento.mla_func_resp_cad,
      trim(
         case
            when evento.cod_tipo_evento = 'CULTUR' then 'CULTURA'
            when evento.cod_tipo_evento = 'ESPORT' then 'ESPORTE'
            when evento.cod_tipo_evento = 'LAZER' then 'LAZER'
            when evento.cod_tipo_evento = 'CURSOS' then 'EDUCAÇÃO'
         end
      ) as cod_tipo_evento,
      evento.des_evento,
      evento.nro_seq_evento || '-' || evento.des_evento as des_evento_new,
      evento.cod_grupo_evento,
      trim(evento.cod_projeto) as cod_projeto,
      evento.dat_ult_alteracao,
      evento.nro_seq_evento || ' ' || tmp_local.nom_local || '  ' || nvl(aluguel_local.des_observacao, '') as obs_aluguel,
      aluguel_local.vlr_aluguel,
      SinalPagamento.vlr_sinal,
      (aluguel_local.vlr_aluguel - SinalPagamento.vlr_sinal) as vlr_restante,
      aluguel_local.vlr_outros
   from {linha_log.banco}:reserva_local
   inner join {linha_log.banco}:evento
      on evento.nro_seq_evento = reserva_local.nro_seq_evento
   inner join {linha_log.banco}:tmp_local
      on tmp_local.nro_seq_local = reserva_local.nro_seq_local
      and tmp_local.cod_unidade = reserva_local.cod_unidade
   left join {linha_log.banco}:aluguel_local
      on aluguel_local.nro_seq_reserva = reserva_local.nro_seq_reserva
   left join (
      select
         aluguel_local_pagamento.nro_seq_reserva,
         sum(nvl(aluguel_local_pagamento.vlr_parcela, 0)) as vlr_sinal
      from {linha_log.banco}:aluguel_local_pagamento
      where aluguel_local_pagamento.nro_parcelas = 1
      group by aluguel_local_pagamento.nro_seq_reserva
   ) SinalPagamento
      on SinalPagamento.nro_seq_reserva = reserva_local.nro_seq_reserva
   where reserva_local.nro_seq_reserva = ?
      and reserva_local.dat_mobilizacao < '31/12/2029'
      and reserva_local.dat_inic_evento < '31/12/2029'
   """,(
      chave.nro_seq_reserva,
   ))

   Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
   linha = cr_ifx.fetchone()
   origem = Linha(*linha) if linha else None

   if origem is None:
       cr_sql.close()
       return

   projetos_validos = {
      'EDUCACAO': ('AULAS','ATIVIDADES','RESERVLOC'),
      'CULTURA': ('RESERVLOC','EVENTOCULT'),
      'ESPORTE': ('RESERVLOC','PROVAJOGO','TREINAMENT'),
      'LAZER':  ('RESERVLOC',)
   }


   if origem.cod_tipo_evento is not None:
      if origem.cod_projeto not in projetos_validos[origem.cod_tipo_evento]:
         cr_sql.close()
         return

   cr_sql.execute("""
   select IdArea
   from Area
   where NomeArea = ?
   """, (origem.cod_tipo_evento,))

   idArea = cr_sql.fetchval()

   if idArea is None:
      print(f"Área não encontrada: {origem.cod_tipo_evento}")
      cr_sql.close()
      return

   cr_sql.execute("""
   select top 1 CategoriaReserva.IdCategoriaReserva
   from CategoriaReserva
   inner join TipoReserva on TipoReserva.IdTipoReserva = CategoriaReserva.IdTipoReserva
   inner join Area on Area.IdArea = TipoReserva.IdArea
   where Area.NomeArea = ?
   and TipoReserva.cod_projeto = ?
   and CategoriaReserva.cod_grupo_evento = ?
   """, (origem.cod_tipo_evento, origem.cod_projeto, origem.cod_grupo_evento,))

   idCategoriaReserva = cr_sql.fetchval()

   if idCategoriaReserva is None:
      print(f"Categoria não encontrada: NomeArea={origem.cod_tipo_evento!r} | TipoReserva={origem.cod_projeto!r} | CategoriaReserva={origem.cod_grupo_evento!r}")
      cr_sql.close()
      return

   wDataMobilizacao = junta_data_hora(
    origem.dat_mobilizacao,
    origem.hor_mobilizacao
   )

   wDataDesmobilizacao = junta_data_hora(
    origem.dat_desmobilizacao,
    origem.hor_desmobilizacao
   )

   wDataInicEvento = junta_data_hora(
    origem.dat_inic_evento,
    origem.hor_inic_evento
   )

   wDataFimEvento = junta_data_hora(
    origem.dat_fim_evento,
    origem.hor_fim_evento
   )

   wDataCancelamento = junta_data_hora(
    origem.dat_cancelamento,
    origem.hor_cancelamento
   )

   cr_sql.execute("""
      update LocalReserva set
      DataMobilizacao = ?,
      DataDesmobilizacao = ?,
      DataInicio = ?,
      DataFim = ?,
      PermiteCompartilhamento = ?,
      CodigoCentroCusto = ?,
      DataCancelamento = ?,
      ObservacaoCancelamento = ?,
      IdUsuarioReserva = isnull((select top 1 IdUsuario from Usuario where Matricula = ?),1302),
      DataReserva = ?,
      TituloReserva = ?,
      IdCategoriaReserva = (
         select top 1 CategoriaReserva.IdCategoriaReserva
         from CategoriaReserva
         inner join TipoReserva on TipoReserva.IdTipoReserva = CategoriaReserva.IdTipoReserva
         inner join Area on Area.IdArea = TipoReserva.IdArea
         where Area.NomeArea = ?
         and TipoReserva.Nome = ?
         and CategoriaReserva.Nome = ?
      ),
      Observacao = ?,
      ValorAluguel = ?,
      ValorEntrada = ?,
      ValorRestante = ?,
      ValorOutros = ?
      where
         IdLocalReserva = (select top 1 PkSql from PkDePara where Tabela = 'LocalReserva' and PkIfx = ?)
   """,(
         wDataMobilizacao,
         wDataDesmobilizacao,
         wDataInicEvento,
         wDataFimEvento,
         origem.idc_aceita_comp,
         origem.cod_centro_custo,
         wDataCancelamento,
         origem.des_cancelamento,
         origem.mla_func_resp_cad,
         wDataInicEvento,
         origem.des_evento,          # TituloReserva
         origem.cod_tipo_evento,     # Area.NomeArea
         origem.cod_projeto,         # TipoReserva.Nome
         origem.cod_grupo_evento,    # CategoriaReserva.Nome
         origem.obs_aluguel,
         origem.vlr_aluguel,
         origem.vlr_sinal,
         origem.vlr_restante,
         origem.vlr_outros,
         linha_log.pk
   ))

   if cr_sql.rowcount == 0:
      cr_sql.execute('begin transaction')
      print(origem.nro_seq_reserva)

      cr_sql.execute(f"""
         insert into LocalReserva
         (
            DataMobilizacao,
            DataDesmobilizacao,
            DataInicio,
            DataFim,
            PermiteCompartilhamento,
            CodigoCentroCusto,
            DataCancelamento,
            ObservacaoCancelamento,
            IdUsuarioReserva,
            DataReserva,
            TituloReserva,
            IdCategoriaReserva,
            Observacao,
            ValorAluguel,
            ValorEntrada,
            ValorRestante,
            ValorOutros
         ) values (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            isnull((select top 1 IdUsuario from Usuario where Matricula = ?),1302),
            ?,
            ?,
            (
               select top 1 CategoriaReserva.IdCategoriaReserva
               from CategoriaReserva
               inner join TipoReserva on TipoReserva.IdTipoReserva = CategoriaReserva.IdTipoReserva
               inner join Area on Area.IdArea = TipoReserva.IdArea
               where Area.NomeArea = ?
               and TipoReserva.Nome = ?
               and CategoriaReserva.Nome = ?
            ),
            ?,
            ?,
            ?,
            ?,
            ?
         )
      """,(
         wDataMobilizacao,
         wDataDesmobilizacao,
         wDataInicEvento,
         wDataFimEvento,
         origem.idc_aceita_comp,
         origem.cod_centro_custo,
         wDataCancelamento,
         origem.des_cancelamento,
         origem.mla_func_resp_cad,    # IdUsuarioReserva
         wDataInicEvento,             # DataReserva
         origem.des_evento,           # TituloReserva
         origem.cod_tipo_evento,      # Area.NomeArea
         origem.cod_projeto,          # TipoReserva.Nome
         origem.cod_grupo_evento,     # CategoriaReserva.Nome
         origem.obs_aluguel,          # Observacao
         origem.vlr_aluguel,
         origem.vlr_sinal,
         origem.vlr_restante,
         origem.vlr_outros
      ))

      cr_sql.execute("""select ident_current('LocalReserva')""")
      pkIdLocalReserva = cr_sql.fetchval()

      cr_sql.execute("insert into PkDePara values ('LocalReserva',?,?)",(pkIdLocalReserva, linha_log.pk,))
      cr_sql.execute("commit transaction")

      cr_sql.execute('begin transaction')

      cr_sql.execute(f"""
         insert into OrganizadorLocalReserva
         (
            IdLocalReserva,
            IdUsuario
         ) values (
            ?,
            isnull((select top 1 IdUsuario from Usuario where Matricula = ?),1302)
         )
      """,(
         pkIdLocalReserva,
         origem.mla_func_resp_cad
      ))

      cr_sql.execute("""select ident_current('OrganizadorLocalReserva')""")
      pkSql = cr_sql.fetchval()

      cr_sql.execute("insert into PkDePara values ('OrganizadorLocalReserva',?,?)",(pkSql, linha_log.pk,))
      cr_sql.execute("commit transaction")

      cr_sql.execute('begin transaction')
      if origem.idc_cancelado != 'S':
         wAtivo = 1
      else:
         wAtivo = 0

      cr_sql.execute(f"""
         insert into AprovacaoLocalReserva
         (
           IdLocal,
           IdLocalReserva,
           DataResposta,
           IdUsuarioAprovacao,
           Aprovado
         ) values (
           (select top 1 IdLocal from Local
            inner join Unidade on Unidade.NomeUnidade = ? and Local.IdUnidade = Unidade.IdUnidade and Unidade.IdClube = 'MTC'
            where Local.NomeLocal = ?),
           ?,
           getdate(),
           isnull((select top 1 IdUsuario from Usuario where Matricula = ?),1302),
           ?
         )
         """,(
            origem.unidade,
            origem.nom_local,
            pkIdLocalReserva,
            origem.mla_func_resp_cad,
            wAtivo
            ))

      cr_sql.execute("""select ident_current('AprovacaoLocalReserva')""")
      pkSql = cr_sql.fetchval()

      cr_sql.execute("insert into PkDePara values ('AprovacaoLocalReserva',?,?)",(pkSql, linha_log.pk,))
      cr_sql.execute("commit transaction")

      cr_ifx.execute(f"""
        select
           vlr_parcela
          ,nro_parcelas
          ,dat_vencimento
          ,dat_pagamento
          ,des_pagamento
        from {linha_log.banco}:aluguel_local_pagamento lp
        where
            lp.nro_seq_reserva = ?
        """,(
            chave.nro_seq_reserva,
      ))
      Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

      for origem in [Linha(*l) for l in cr_ifx]:
         wDataVencimento = datetime.strptime(origem.dat_vencimento.strftime("%d/%m/%Y"), "%d/%m/%Y")
         if origem.dat_pagamento:
            wDataPagamento = datetime.strptime(origem.dat_pagamento.strftime("%d/%m/%Y"), "%d/%m/%Y")
         else:
            wDataPagamento = None
         cr_sql.execute('begin transaction')
         cr_sql.execute("""
            insert into PagamentoLocalReserva
            (
               IdLocalReserva,
               ValorParcela,
               NumeroParcela,
               DataVencimento,
               DataPagamento,
               DescricaoPagamento
            ) values (
               ?,
               ?,
               ?,
               ?,
               ?,
               ?
            )
         """,(
            pkIdLocalReserva,
            origem.vlr_parcela,
            origem.nro_parcelas,
            wDataVencimento,
            wDataPagamento,
            origem.des_pagamento
         ))
         cr_sql.execute("commit transaction")

   cr_sql.close()


# Teste
#
if __name__ == "__main__":
    import sys
    from conexoes import *

    ifx = conecta_informix('minas')
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
            tabela = 'reserva_local'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        convert(ifx, sql, linha)