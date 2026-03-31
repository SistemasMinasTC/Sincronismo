#!/usr/bin/python
#
# reserva local - Rogerio Peter em 30/10/2025 - 15:35h

from recordtype import recordtype
import os
from datetime import datetime

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
   rl.nro_seq_reserva
  ,rl.cod_unidade
  ,case when rl.cod_unidade=1 then 'MINAS 1'
        when rl.cod_unidade=2 then 'MINAS 2'
        when rl.cod_unidade=3 then 'MINAS NAUTICO'
        when rl.cod_unidade=4 then 'MINAS COUNTRY'
        end unidade
  ,rl.nro_seq_local
  ,lc.nom_local
  ,rl.dat_inic_evento
  ,rl.dat_fim_evento
  ,rl.hor_inic_evento
  ,rl.hor_fim_evento
  ,rl.dat_mobilizacao
  ,rl.dat_desmobilizacao
  ,rl.hor_mobilizacao
  ,rl.hor_desmobilizacao
  ,case when rl.idc_aceita_comp = 'S' then 1 else 0 end idc_aceita_comp
  ,rl.idc_cancelado
  ,rl.des_cancelamento
  ,rl.dat_cancelamento
  ,rl.hor_cancelamento
  ,ev.cod_centro_custo
  ,ev.mla_func_resp_exec
  ,ev.mla_func_resp_cad
  ,case when ev.cod_tipo_evento = 'CULTUR' then 'Cultural'
       when ev.cod_tipo_evento = 'LAZER' then 'Lazer'
       when ev.cod_tipo_evento = 'ESPORT' then 'Esportes'
	   when ev.cod_tipo_evento = 'SOCIAL' then 'Social'
       when ev.cod_tipo_evento = 'EVECOR' then 'Evento Corporativo'
       when ev.cod_tipo_evento = 'FES15A' then 'Festas'
       when ev.cod_tipo_evento = 'OUTROS' then 'Outros'
       when ev.cod_tipo_evento = 'CURSOS' then 'Curso'
       when ev.cod_tipo_evento = 'CONGRE' then 'Congresso'
       when ev.cod_tipo_evento = 'CASAM' then 'Casamento'
       when ev.cod_tipo_evento = 'COQUE' then 'Conquetel'
       when ev.cod_tipo_evento = 'BAILE' then 'Baile'
       when ev.cod_tipo_evento = 'RECHUM' then 'Recursos Humanos'
       when ev.cod_tipo_evento = 'ANIVER' then 'Aniversario'
	   end  cod_tipo_evento
  ,ev.des_evento des_evento
  ,ev.nro_seq_evento||'-'||ev.des_evento des_evento_new
  ,ev.cod_grupo_evento
  ,dat_ult_alteracao
  ,ev.nro_seq_evento||' '||lc.nom_local||'  '||nvl(al.des_observacao,'') obs_aluguel
  ,al.vlr_aluguel
  ,si.vlr_sinal
  ,(al.vlr_aluguel-si.vlr_sinal) vlr_restante
  ,al.vlr_outros
from {linha_log.banco}:reserva_local rl
 inner join {linha_log.banco}:evento ev on ev.nro_seq_evento = rl.nro_seq_evento
 inner join {linha_log.banco}:tmp_local lc on lc.nro_seq_local = rl.nro_seq_local and rl.cod_unidade = lc.cod_unidade
 left  join {linha_log.banco}:aluguel_local al  on al.nro_seq_reserva = rl.nro_seq_reserva
left  join (select nro_seq_reserva, sum(nvl(vlr_parcela,0)) vlr_sinal from {linha_log.banco}:aluguel_local_pagamento where nro_parcelas = 1 group by nro_seq_reserva ) si on si.nro_seq_reserva = rl.nro_seq_reserva
where 
       rl.nro_seq_reserva = ?
       and rl.dat_mobilizacao < '31/12/2029'
       and rl.dat_inic_evento < '31/12/2029'
   """,(
      chave.nro_seq_reserva,
   ))

   Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
   linha = cr_ifx.fetchone()
   origem = Linha(*linha) if linha else None

   if origem is None:
       cr_sql.close()
       return

   if not origem:
        cr_sql.close()
        return

   wDataMobilizacao    = datetime.strptime(origem.dat_mobilizacao.strftime("%d/%m/%Y") + " "+origem.hor_mobilizacao.strftime("%H:%M:%S"), "%d/%m/%Y %H:%M:%S")
   wDataDesmobilizacao = datetime.strptime(origem.dat_desmobilizacao.strftime("%d/%m/%Y")+" "+origem.hor_desmobilizacao.strftime("%H:%M:%S"), "%d/%m/%Y %H:%M:%S")
   wDataInicEvento     = datetime.strptime(origem.dat_inic_evento.strftime("%d/%m/%Y")+" "+origem.hor_inic_evento.strftime("%H:%M:%S"), "%d/%m/%Y %H:%M:%S")
   wDataFimEvento      = datetime.strptime(origem.dat_fim_evento.strftime("%d/%m/%Y")+" "+origem.hor_fim_evento.strftime("%H:%M:%S"), "%d/%m/%Y %H:%M:%S")

   wcod_grupo_evento = 0
   if origem.cod_tipo_evento == 'Cultural':
       if origem.cod_grupo_evento=='EVENTO':
           wcod_grupo_evento = 1
       elif origem.cod_grupo_evento=='MONTAGEM':
           wcod_grupo_evento = 2
       elif origem.cod_grupo_evento=='FOLGA':
           wcod_grupo_evento = 3
       elif origem.cod_grupo_evento=='OUTROS':
           wcod_grupo_evento = 4
       elif origem.cod_grupo_evento=='CANCELADO':
           wcod_grupo_evento = 5
       else:
           wcod_grupo_evento = 6

   cr_sql.execute("""
      update ge.LocalReserva set
      DataMobilizacao= ?,
      DataDesmobilizacao= ?,
      DataInicio= ?,
      DataFim= ?,
      PermiteCompartilhamento= ?,
      CodigoCentroCusto= ?,
      DataCancelamento= ?,
      ObservacaoCancelamento= ?,
      IdUsuarioReserva= (select IdUsuario from Usuario Where Matricula = ?),
      DataReserva= ?,
      Descricao= ?,
      IdEventoTipo = (select IdEventoTipo from evento.EventoTipo Where NomeTipo = ?),
      IdUsuarioResponsavel = (select IdUsuario from Usuario Where Matricula = ?),
      Observacao = ?,
      ValorAluguel = ?,
      ValorEntrada = ?,
      ValorRestante = ?,
      ValorOutros = ?
      where
         IdLocalReserva = (select PkSql from PkDePara where Tabela = 'LocalReserva' and PkIfx = ?)
   """,(
         wDataMobilizacao,
         wDataDesmobilizacao,
         wDataInicEvento,
         wDataFimEvento,
         origem.idc_aceita_comp,
         origem.cod_centro_custo,
         origem.dat_cancelamento,
         origem.des_cancelamento,
         origem.mla_func_resp_cad,
         origem.dat_inic_evento,
         origem.des_evento,
         origem.cod_tipo_evento,
         origem.mla_func_resp_exec,
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
         insert into ge.LocalReserva
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
            Descricao,
            IdEventoTipo,
            IdUsuarioResponsavel,
            IdGrupoEvento,
            Observacao,
            ValorAluguel,
            ValorEntrada,
            ValorRestante,
            ValorOutros
            
         )  values (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            isnull((select IdUsuario from Usuario Where Matricula = ?),1302) ,
            ?,
            ?,
            (select IdEventoTipo from evento.EventoTipo Where NomeTipo = ?),
            isnull((select IdUsuario from Usuario Where Matricula = ?),1302) ,
            (select IdGrupoEvento from evento.EventoGrupo Where NomeGrupo = ?),
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
         origem.dat_cancelamento,
         origem.des_cancelamento,
         origem.mla_func_resp_cad,
         origem.dat_inic_evento,
         origem.des_evento,
         origem.cod_tipo_evento,
         origem.mla_func_resp_exec,
        #wcod_grupo_evento
         origem.cod_grupo_evento,
         origem.obs_aluguel,
         origem.vlr_aluguel,
         origem.vlr_sinal,
         origem.vlr_restante,
         origem.vlr_outros
      ))

      cr_sql.execute("""select ident_current('ge.LocalReserva')""")
      pkIdLocalReserva = cr_sql.fetchval()

      cr_sql.execute("insert into PkDePara values ('LocalReserva',?,?)",(pkIdLocalReserva, linha_log.pk,))
      cr_sql.execute("commit transaction")

      # [ge].[LocalReservaOrganizador] insert dos organizadores
      cr_sql.execute('begin transaction')

      cr_sql.execute(f"""
         insert into ge.LocalReservaOrganizador
         (
            IdLocalReserva,
            IdUsuario 
         ) values (
            ?,
            isnull((select IdUsuario from Usuario Where Matricula = ?),1302)
         )
      """,(
         pkIdLocalReserva,
         origem.mla_func_resp_cad
      ))

      cr_sql.execute("""select ident_current('ge.LocalReservaOrganizador')""")
      pkSql = cr_sql.fetchval()

      cr_sql.execute("insert into PkDePara values ('LocalReservaOrganizador',?,?)",(pkSql, linha_log.pk,))
      cr_sql.execute("commit transaction")
      # validação do Local


      #cr_ifx.execute(f"""
      #     select IdLocal from Local Lc
      #     inner join Unidade un on un.NomeUnidade = ? and Lc.IdUnidade = un.IdUnidade and un.IdClube='MTC'
      #     where NomeLocal = ? )
      #""",(
      #      origem.unidade,
      #      origem.nom_local
      #))
      #linha = cr_ifx.fetchone()
      #origemLocal = Linha(*linha)if linha else None

      #wdata = datetime.now()
      #msg = f"{wdata}| unidade: {origem.unidade} | local: {origem.nom_local}kk\n"
      #with open("reserva_local.log", "a") as f:
      #     f.write(msg)

      #if origemLocal is None:
      #    wdata = datetime.now()
      #    msg = f"{wdata}| unidade: {origem.unidade} | local: {origem.nom_local}kk\n"
      #    with open("reserva_local.log", "a") as f:
      #        f.write(msg)


      #if origemLocal.IdLocal == 0:
      #    wdata = datetime.now()
      #    msg = f"{wdata}| unidade: {origem.unidade} | local: {origem.nom_local}kk\n"
      #    with open("reserva_local.log", "a") as f:
      #        f.write(msg)
         

      # [ge].[LocalReservaLocalAprovacao]
      cr_sql.execute('begin transaction')
      # todos os lançamentos aprovados
      if origem.idc_cancelado != 'S':
         wAtivo = 1
      else:
         wAtivo = 0

      cr_sql.execute(f"""
         insert into ge.LocalReservaLocalAprovacao
         (
           IdLocal,
           IdLocalReserva,
           DataResposta,
           IdUsuarioAprovacao,
           Aprovado
         ) values (
           (select top 1 IdLocal from Local Lc
           inner join Unidade un on un.NomeUnidade = ? and Lc.IdUnidade = un.IdUnidade and un.IdClube='MTC'
           where NomeLocal = ? ),
           ?,
           getdate(),
           isnull((select IdUsuario from Usuario Where Matricula = ?),1302),
           ?
         )
         """,(
            origem.unidade,
            origem.nom_local,
            pkIdLocalReserva,
            origem.mla_func_resp_cad,
            wAtivo
            ))

      cr_sql.execute("""select ident_current('ge.LocalReservaLocalAprovacao')""")
      pkSql = cr_sql.fetchval()

      cr_sql.execute("insert into PkDePara values ('LocalReservaLocalAprovacao',?,?)",(pkSql, linha_log.pk,))
      cr_sql.execute("commit transaction")

      # [ge].[LocalReservaPagamento] insert parcelas 
      # cr_ifx = ifx.cursor()
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
         wDataPagamento  = datetime.strptime(origem.dat_pagamento.strftime("%d/%m/%Y"), "%d/%m/%Y")
         cr_sql.execute('begin transaction')
         cr_sql.execute("""
            insert into ge.LocalReservaPagamento
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

# [ge].[LocalReservaOrganizador]
# [ge].[LocalReservaLocalAprovacao]

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
        # print(linha)
        convert(ifx, sql, linha)
