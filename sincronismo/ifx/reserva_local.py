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
   elif linha_log.banco == 'serra':
        cod_clube = 'MSDR'

   linha_log.pk = cod_clube+'|'+'reserva_local|'+linha_log.pk

   Chave = recordtype('Chave','cod_clube, tabela, nro_seq_reserva')
   chave = Chave(*linha_log.pk.split('|'))


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
         when reversa_local.cod_unidade = 9 then 'LOCAIS EXTERNOS'
      end as Unidade,
      reserva_local.nro_seq_local,
      trim(local.nom_local) as nom_local,
      to_char(reserva_local.dat_inic_evento, '%Y-%m-%d ') || to_char(reserva_local.hor_inic_evento, '%H:%M:%S') as dat_inic_evento,
      to_char(reserva_local.dat_fim_evento, '%Y-%m-%d ') || to_char(reserva_local.hor_fim_evento, '%H:%M:%S') as dat_fim_evento,
      to_char(reserva_local.dat_mobilizacao, '%Y-%m-%d ') || to_char(reserva_local.hor_mobilizacao, '%H:%M:%S') as dat_mobilizacao,
      to_char(reserva_local.dat_desmobilizacao, '%Y-%m-%d ') || to_char(reserva_local.hor_desmobilizacao, '%H:%M:%S') as dat_desmobilizacao,
      case
         when reserva_local.idc_aceita_comp = 'S' then 1
         else 0
      end as idc_aceita_comp,
      reserva_local.idc_cancelado,
      reserva_local.des_cancelamento,
      to_char(reserva_local.dat_cancelamento, '%Y-%m-%d ') || to_char(reserva_local.hor_cancelamento, '%H:%M:%S') as dat_cancelamento,
      evento.cod_centro_custo,
      evento.mla_func_resp_exec,
      evento.mla_func_resp_cad,
      trim(
         case
            when evento.cod_tipo_evento = '83142' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'ALUGUE' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'ANIVER' then 'LAZER'
            when evento.cod_tipo_evento = 'AULA' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'AULESP' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'BAILE' then 'LAZER'
            when evento.cod_tipo_evento = 'BODAS' then 'LAZER'
            when evento.cod_tipo_evento = 'CAMIN' then 'LAZER'
            when evento.cod_tipo_evento = 'CAMPEO' then 'ESPORTE'
            when evento.cod_tipo_evento = 'CASAM' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'CBJUDO' then 'ESPORTE'
            when evento.cod_tipo_evento = 'CONGRE' then 'ESPORTE'
            when evento.cod_tipo_evento = 'COQUE' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'CULTUR' then 'CULTURA'
            when evento.cod_tipo_evento = 'CURSO' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'CURSOS' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'DANCA' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'EDUCA' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'ENSAI' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'ESPOR' then 'ESPORTE'
            when evento.cod_tipo_evento = 'ESPORT' then 'ESPORTE'
            when evento.cod_tipo_evento = 'ESTAC' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'EVECOR' then 'LAZER'
            when evento.cod_tipo_evento = 'FES15A' then 'LAZER'
            when evento.cod_tipo_evento = 'FESDAN' then 'LAZER'
            when evento.cod_tipo_evento = 'FESTA' then 'LAZER'
            when evento.cod_tipo_evento = 'FESTAC' then 'LAZER'
            when evento.cod_tipo_evento = 'HEBRAI' then 'LAZER'
            when evento.cod_tipo_evento = 'INFANT' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'IOGATC' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'JANT' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'JANTAR' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'LAZER' then 'LAZER'
            when evento.cod_tipo_evento = 'LUDICO' then 'LAZER'
            when evento.cod_tipo_evento = 'MANUT' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'MARKET' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'MEDICO' then 'ESPORTE'
            when evento.cod_tipo_evento = 'OUTROS' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'PALEST' then 'CULTURA'
            when evento.cod_tipo_evento = 'PARCE' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'PEDAGO' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'PEDALA' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'PRERES' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'RECHUM' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'RECRE' then 'LAZER'
            when evento.cod_tipo_evento = 'RECREA' then 'LAZER'
            when evento.cod_tipo_evento = 'RECSOC' then 'LAZER'
            when evento.cod_tipo_evento = 'RESDIR' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'RESERV' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'REUNIA' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'REVEIL' then 'LAZER'
            when evento.cod_tipo_evento = 'SHOW' then 'LAZER'
            when evento.cod_tipo_evento = 'SOCIAL' then 'LAZER'
            when evento.cod_tipo_evento = 'TENESP' then 'ESPORTE'
            when evento.cod_tipo_evento = 'TESTE' then 'ESPORTE'
            when evento.cod_tipo_evento = 'TREINA' then 'ESPORTE'
            when evento.cod_tipo_evento = 'VENDA' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'VENDC' then 'ADMINISTRATIVO'
            when evento.cod_tipo_evento = 'VENUNF' then 'EDUCAÇÃO'
            when evento.cod_tipo_evento = 'VENVIA' then 'ADMINISTRATIVO'
         else 'ADMINISTRATIVO'
         end
      ) as cod_tipo_evento,
      evento.des_evento,
      evento.nro_seq_evento || '-' || evento.des_evento as des_evento_new,
      trim(evento.cod_grupo_evento) as cod_grupo_evento,
      trim(grupo_evento.des_grupo_evento) as des_grupo_evento,
      trim(evento.cod_projeto) as cod_projeto,
      evento.dat_ult_alteracao,
      evento.nro_seq_evento || ' ' || local.nom_local || '  ' || nvl(aluguel_local.des_observacao, '') as obs_aluguel,
      aluguel_local.vlr_aluguel,
      SinalPagamento.vlr_sinal,
      (aluguel_local.vlr_aluguel - SinalPagamento.vlr_sinal) as vlr_restante,
      aluguel_local.vlr_outros
   from {linha_log.banco}:reserva_local
   inner join {linha_log.banco}:evento on evento.nro_seq_evento = reserva_local.nro_seq_evento
   inner join {linha_log.banco}:local on local.nro_seq_local = reserva_local.nro_seq_local and 
   local.cod_unidade = reserva_local.cod_unidade
   inner join {linha_log.banco}:grupo_evento on grupo_evento.cod_projeto = evento.cod_projeto and 
   grupo_evento.cod_grupo_evento = evento.cod_grupo_evento
   left join {linha_log.banco}:aluguel_local on aluguel_local.nro_seq_reserva = reserva_local.nro_seq_reserva
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
   """,(
      chave.nro_seq_reserva,
   ))

   Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
   linha = cr_ifx.fetchone()
   origem = Linha(*linha) if linha else None

   if origem is None:
       cr_sql.close()
       return
   
   cr_sql.execute("""
   select top 1
      CategoriaReserva.IdCategoriaReserva
   from CategoriaReserva
   inner join TipoReserva on TipoReserva.IdTipoReserva = CategoriaReserva.IdTipoReserva
   inner join Area on Area.IdArea = TipoReserva.IdArea
   where Area.NomeArea = ?
   and TipoReserva.cod_projeto = ?
   and CategoriaReserva.cod_grupo_evento = ?
   """,(
      origem.cod_tipo_evento,
      origem.cod_projeto,
      origem.cod_grupo_evento,
   ))

   idCategoriaReserva = cr_sql.fetchval()

   if idCategoriaReserva is None:
      print(
         f"Categoria não encontrada: "
         f"{origem.cod_tipo_evento} | "
         f"{origem.cod_projeto} | "
         f"{origem.cod_grupo_evento}. "
         f"Gravando IdCategoriaReserva = NULL."
      )
      idCategoriaReserva = None


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
      IdCategoriaReserva = ?,
      Observacao = ?,
      ValorAluguel = ?,
      ValorEntrada = ?,
      ValorRestante = ?,
      ValorOutros = ?
   where nro_seq_reserva = ?
   """,(
    origem.dat_mobilizacao,
    origem.dat_desmobilizacao,
    origem.dat_inic_evento,
    origem.dat_fim_evento,
    origem.idc_aceita_comp,
    origem.cod_centro_custo,
    origem.dat_cancelamento,
    origem.des_cancelamento,
    origem.mla_func_resp_cad,
    origem.dat_inic_evento,
    origem.des_evento,
    idCategoriaReserva,
    origem.obs_aluguel,
    origem.vlr_aluguel,
    origem.vlr_sinal,
    origem.vlr_restante,
    origem.vlr_outros,
    origem.nro_seq_reserva
))

   if cr_sql.rowcount == 0:

      cr_sql.execute('begin transaction')

      cr_sql.execute(f"""
         insert into LocalReserva
         (
            nro_seq_reserva,
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
            ?,
            isnull((select top 1 IdUsuario from Usuario where Matricula = ?),1302),
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
         )
      """,(
         origem.nro_seq_reserva,
         origem.dat_mobilizacao,
         origem.dat_desmobilizacao,
         origem.dat_inic_evento,
         origem.dat_fim_evento,
         origem.idc_aceita_comp,
         origem.cod_centro_custo,
         origem.dat_cancelamento,
         origem.des_cancelamento,
         origem.mla_func_resp_cad,    # IdUsuarioReserva
         origem.dat_inic_evento,      # DataReserva
         origem.des_evento,           # TituloReserva
         idCategoriaReserva,
         origem.obs_aluguel,          # Observacao
         origem.vlr_aluguel,
         origem.vlr_sinal,
         origem.vlr_restante,
         origem.vlr_outros
      ))

      cr_sql.execute("""
      select IdLocalReserva
      from LocalReserva
      where nro_seq_reserva = ?
      """, (
         origem.nro_seq_reserva,
      ))

      IdLocalReserva = cr_sql.fetchval()


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
         IdLocalReserva,
         origem.mla_func_resp_cad
      ))

      
      if origem.idc_cancelado != 'S':
         wAtivo = 1
      else:
         wAtivo = 0

      cr_sql.execute("""
      select top 1 Local.IdLocal
      from Local
      inner join Unidade on Unidade.IdUnidade = Local.IdUnidade
      where Unidade.NomeUnidade = ? and
      Unidade.IdClube = ? and
      LTRIM(RTRIM(Local.NomeLocal)) = ?
      """, (
         origem.unidade,
         cod_clube,
         origem.nom_local
      ))

      idLocal = cr_sql.fetchval()

      if idLocal is None:
         print(
            f"Local não encontrado: "
            f"{origem.unidade} | {cod_clube} | {origem.nom_local}"
         )
      else:
         cr_sql.execute(f"""
         insert into LocaisReservados
         (
           IdLocal,
           IdLocalReserva,
           DataResposta,
           Aprovado
         ) values (?, ?, getdate(), ?)
         """,(
            idLocal,
            IdLocalReserva,
            wAtivo
         ))
      
      cr_sql.execute("commit transaction")

      cr_ifx.execute(f"""
        select
            vlr_parcela,
            nro_parcelas,
            dat_vencimento,
            dat_pagamento,
            des_pagamento
        from {linha_log.banco}:aluguel_local_pagamento lp
        where
            lp.nro_seq_reserva = ?
        """,(
            chave.nro_seq_reserva,
      ))
      Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

      for origem in [Linha(*l) for l in cr_ifx]:
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
            IdLocalReserva,
            origem.vlr_parcela,
            origem.nro_parcelas,
            origem.dat_vencimento,
            origem.dat_pagamento,
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
      try:
        convert(ifx, sql, linha)
      except Exception as erro:
         print(erro)
