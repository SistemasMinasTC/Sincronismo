----------------------------------------------------------------------------------------
--- _cota_
---

database minas;

create trigger if not exists mc_insert__cota_ insert on _cota_ referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '_cota_',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert__cota_;

create trigger if not exists mc_insert__cota_ insert on _cota_ referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '_cota_',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update__cota_;

create trigger if not exists mc_update__cota_ update on _cota_ referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '_cota_',
            'upd',
            novo.cod_tipo_associado||'|'||novo.cod_cota,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete__cota_;

create trigger  if not exists mc_delete__cota_ delete on _cota_ referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '_cota_',
            'del',
            velho.cod_tipo_associado||'|'||velho.cod_cota,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- _movimentacao_receita_
---

database minas;

create trigger if not exists mc_insert__movimentacao_receita_ insert on _movimentacao_receita_ referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '_movimentacao_receita_',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_associado||'|'||novo.cod_receita||'|'||novo.dat_receita||'|'||novo.hor_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert__movimentacao_receita_;

create trigger if not exists mc_insert__movimentacao_receita_ insert on _movimentacao_receita_ referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '_movimentacao_receita_',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_associado||'|'||novo.cod_receita||'|'||novo.dat_receita||'|'||novo.hor_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update__movimentacao_receita_;

create trigger if not exists mc_update__movimentacao_receita_ update on _movimentacao_receita_ referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '_movimentacao_receita_',
            'upd',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_associado||'|'||novo.cod_receita||'|'||novo.dat_receita||'|'||novo.hor_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete__movimentacao_receita_;

create trigger  if not exists mc_delete__movimentacao_receita_ delete on _movimentacao_receita_ referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '_movimentacao_receita_',
            'del',
            velho.cod_tipo_associado||'|'||velho.cod_cota||'|'||velho.cod_associado||'|'||velho.cod_receita||'|'||velho.dat_receita||'|'||velho.hor_receita,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- acompanhante
---

database minas;

create trigger if not exists mc_insert_acompanhante insert on acompanhante referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acompanhante',
            'ins',
            novo.nro_seq_acomp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_acompanhante;

create trigger if not exists mc_insert_acompanhante insert on acompanhante referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acompanhante',
            'ins',
            novo.nro_seq_acomp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_acompanhante;

create trigger if not exists mc_update_acompanhante update on acompanhante referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acompanhante',
            'upd',
            novo.nro_seq_acomp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_acompanhante;

create trigger  if not exists mc_delete_acompanhante delete on acompanhante referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acompanhante',
            'del',
            velho.nro_seq_acomp,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- agregado
---

database minas;

create trigger if not exists mc_insert_agregado insert on agregado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'agregado',
            'ins',
            novo.nro_seq_agregado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_agregado;

create trigger if not exists mc_insert_agregado insert on agregado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'agregado',
            'ins',
            novo.nro_seq_agregado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_agregado;

create trigger if not exists mc_update_agregado update on agregado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'agregado',
            'upd',
            novo.nro_seq_agregado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_agregado;

create trigger  if not exists mc_delete_agregado delete on agregado referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'agregado',
            'del',
            velho.nro_seq_agregado,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- associado
---

database minas;

create trigger if not exists mc_insert_associado insert on associado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado',
            'ins',
            novo.cod_associado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_associado;

create trigger if not exists mc_insert_associado insert on associado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado',
            'ins',
            novo.cod_associado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_associado;

create trigger if not exists mc_update_associado update on associado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado',
            'upd',
            novo.cod_associado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_associado;

create trigger  if not exists mc_delete_associado delete on associado referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado',
            'del',
            velho.cod_associado,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- associado_excl
---

database minas;

create trigger if not exists mc_insert_associado_excl insert on associado_excl referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_excl',
            'ins',
            novo.cod_associado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_associado_excl;

create trigger if not exists mc_insert_associado_excl insert on associado_excl referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_excl',
            'ins',
            novo.cod_associado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_associado_excl;

create trigger if not exists mc_update_associado_excl update on associado_excl referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_excl',
            'upd',
            novo.cod_associado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_associado_excl;

create trigger  if not exists mc_delete_associado_excl delete on associado_excl referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_excl',
            'del',
            velho.cod_associado,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- banco
---

database minas;

create trigger if not exists mc_insert_banco insert on banco referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'banco',
            'ins',
            novo.cod_banco,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_banco;

create trigger if not exists mc_insert_banco insert on banco referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'banco',
            'ins',
            novo.cod_banco,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_banco;

create trigger if not exists mc_update_banco update on banco referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'banco',
            'upd',
            novo.cod_banco,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_banco;

create trigger  if not exists mc_delete_banco delete on banco referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'banco',
            'del',
            velho.cod_banco,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- cidadao
---

database minasdba;

create trigger if not exists mc_insert_cidadao insert on cidadao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cidadao',
            'ins',
            novo.idf_cidadao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_cidadao;

create trigger if not exists mc_insert_cidadao insert on cidadao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cidadao',
            'ins',
            novo.idf_cidadao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_cidadao;

create trigger if not exists mc_update_cidadao update on cidadao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cidadao',
            'upd',
            novo.idf_cidadao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_cidadao;

create trigger  if not exists mc_delete_cidadao delete on cidadao referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cidadao',
            'del',
            velho.idf_cidadao,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- cota_associado
---

database minas;

create trigger if not exists mc_insert_cota_associado insert on cota_associado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_associado',
            'ins',
            novo.cod_associado||'|'||novo.cod_tipo_associado||'|'||novo.cod_cota,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_cota_associado;

create trigger if not exists mc_insert_cota_associado insert on cota_associado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_associado',
            'ins',
            novo.cod_associado||'|'||novo.cod_tipo_associado||'|'||novo.cod_cota,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_cota_associado;

create trigger if not exists mc_update_cota_associado update on cota_associado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_associado',
            'upd',
            novo.cod_associado||'|'||novo.cod_tipo_associado||'|'||novo.cod_cota,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_cota_associado;

create trigger  if not exists mc_delete_cota_associado delete on cota_associado referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_associado',
            'del',
            velho.cod_associado||'|'||velho.cod_tipo_associado||'|'||velho.cod_cota,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- cota_debito_auto
---

database minas;

create trigger if not exists mc_insert_cota_debito_auto insert on cota_debito_auto referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_debito_auto',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_banco,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_cota_debito_auto;

create trigger if not exists mc_insert_cota_debito_auto insert on cota_debito_auto referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_debito_auto',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_banco,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_cota_debito_auto;

create trigger if not exists mc_update_cota_debito_auto update on cota_debito_auto referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_debito_auto',
            'upd',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_banco,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_cota_debito_auto;

create trigger  if not exists mc_delete_cota_debito_auto delete on cota_debito_auto referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_debito_auto',
            'del',
            velho.cod_tipo_associado||'|'||velho.cod_cota||'|'||velho.cod_banco,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- disp_entrada_saida
---

database minas;

create trigger if not exists mc_insert_disp_entrada_saida insert on disp_entrada_saida referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'disp_entrada_saida',
            'ins',
            novo.cod_unidade||'|'||novo.cod_portaria,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_disp_entrada_saida;

create trigger if not exists mc_insert_disp_entrada_saida insert on disp_entrada_saida referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'disp_entrada_saida',
            'ins',
            novo.cod_unidade||'|'||novo.cod_portaria,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_disp_entrada_saida;

create trigger if not exists mc_update_disp_entrada_saida update on disp_entrada_saida referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'disp_entrada_saida',
            'upd',
            novo.cod_unidade||'|'||novo.cod_portaria,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_disp_entrada_saida;

create trigger  if not exists mc_delete_disp_entrada_saida delete on disp_entrada_saida referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'disp_entrada_saida',
            'del',
            velho.cod_unidade||'|'||velho.cod_portaria,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- disp_entrada_saida
---

database minas;

create trigger if not exists mc_insert_disp_entrada_saida insert on disp_entrada_saida referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'disp_entrada_saida',
            'ins',
            novo.cod_unidade||'|'||novo.cod_portaria||'|'||novo.nro_seq_disp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_disp_entrada_saida;

create trigger if not exists mc_insert_disp_entrada_saida insert on disp_entrada_saida referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'disp_entrada_saida',
            'ins',
            novo.cod_unidade||'|'||novo.cod_portaria||'|'||novo.nro_seq_disp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_disp_entrada_saida;

create trigger if not exists mc_update_disp_entrada_saida update on disp_entrada_saida referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'disp_entrada_saida',
            'upd',
            novo.cod_unidade||'|'||novo.cod_portaria||'|'||novo.nro_seq_disp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_disp_entrada_saida;

create trigger  if not exists mc_delete_disp_entrada_saida delete on disp_entrada_saida referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'disp_entrada_saida',
            'del',
            velho.cod_unidade||'|'||velho.cod_portaria||'|'||velho.nro_seq_disp,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- fatura
---

database minas;

create trigger if not exists mc_insert_fatura insert on fatura referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'fatura',
            'ins',
            novo.nro_fatura,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_fatura;

create trigger if not exists mc_insert_fatura insert on fatura referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'fatura',
            'ins',
            novo.nro_fatura,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_fatura;

create trigger if not exists mc_update_fatura update on fatura referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'fatura',
            'upd',
            novo.nro_fatura,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_fatura;

create trigger  if not exists mc_delete_fatura delete on fatura referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'fatura',
            'del',
            velho.nro_fatura,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- item_fat_eterno
---

database minas;

create trigger if not exists mc_insert_item_fat_eterno insert on item_fat_eterno referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'item_fat_eterno',
            'ins',
            novo.nro_fatura||'|'||novo.nro_seq,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_item_fat_eterno;

create trigger if not exists mc_insert_item_fat_eterno insert on item_fat_eterno referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'item_fat_eterno',
            'ins',
            novo.nro_fatura||'|'||novo.nro_seq,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_item_fat_eterno;

create trigger if not exists mc_update_item_fat_eterno update on item_fat_eterno referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'item_fat_eterno',
            'upd',
            novo.nro_fatura||'|'||novo.nro_seq,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_item_fat_eterno;

create trigger  if not exists mc_delete_item_fat_eterno delete on item_fat_eterno referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'item_fat_eterno',
            'del',
            velho.nro_fatura||'|'||velho.nro_seq,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- mot_movto
---

database minasdba;

create trigger if not exists mc_insert_mot_movto insert on mot_movto referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'mot_movto',
            'ins',
            novo.cod_motivo,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_mot_movto;

create trigger if not exists mc_insert_mot_movto insert on mot_movto referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'mot_movto',
            'ins',
            novo.cod_motivo,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_mot_movto;

create trigger if not exists mc_update_mot_movto update on mot_movto referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'mot_movto',
            'upd',
            novo.cod_motivo,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_mot_movto;

create trigger  if not exists mc_delete_mot_movto delete on mot_movto referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'mot_movto',
            'del',
            velho.cod_motivo,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- nacionalidade
---

database minas;

create trigger if not exists mc_insert_nacionalidade insert on nacionalidade referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'nacionalidade',
            'ins',
            novo.cod_nacionalidade,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_nacionalidade;

create trigger if not exists mc_insert_nacionalidade insert on nacionalidade referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'nacionalidade',
            'ins',
            novo.cod_nacionalidade,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_nacionalidade;

create trigger if not exists mc_update_nacionalidade update on nacionalidade referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'nacionalidade',
            'upd',
            novo.cod_nacionalidade,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_nacionalidade;

create trigger  if not exists mc_delete_nacionalidade delete on nacionalidade referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'nacionalidade',
            'del',
            velho.cod_nacionalidade,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- parcela_plano
---

database minas;

create trigger if not exists mc_insert_parcela_plano insert on parcela_plano referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'parcela_plano',
            'ins',
            novo.cod_plano||'|'||novo.nro_parcela,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_parcela_plano;

create trigger if not exists mc_insert_parcela_plano insert on parcela_plano referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'parcela_plano',
            'ins',
            novo.cod_plano||'|'||novo.nro_parcela,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_parcela_plano;

create trigger if not exists mc_update_parcela_plano update on parcela_plano referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'parcela_plano',
            'upd',
            novo.cod_plano||'|'||novo.nro_parcela,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_parcela_plano;

create trigger  if not exists mc_delete_parcela_plano delete on parcela_plano referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'parcela_plano',
            'del',
            velho.cod_plano||'|'||velho.nro_parcela,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- plano_cobranca
---

database minas;

create trigger if not exists mc_insert_plano_cobranca insert on plano_cobranca referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'plano_cobranca',
            'ins',
            novo.cod_plano,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_plano_cobranca;

create trigger if not exists mc_insert_plano_cobranca insert on plano_cobranca referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'plano_cobranca',
            'ins',
            novo.cod_plano,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_plano_cobranca;

create trigger if not exists mc_update_plano_cobranca update on plano_cobranca referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'plano_cobranca',
            'upd',
            novo.cod_plano,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_plano_cobranca;

create trigger  if not exists mc_delete_plano_cobranca delete on plano_cobranca referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'plano_cobranca',
            'del',
            velho.cod_plano,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- portaria
---

database minas;

create trigger if not exists mc_insert_portaria insert on portaria referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'portaria',
            'ins',
            novo.cod_unidade||'|'||novo.cod_portaria,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_portaria;

create trigger if not exists mc_insert_portaria insert on portaria referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'portaria',
            'ins',
            novo.cod_unidade||'|'||novo.cod_portaria,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_portaria;

create trigger if not exists mc_update_portaria update on portaria referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'portaria',
            'upd',
            novo.cod_unidade||'|'||novo.cod_portaria,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_portaria;

create trigger  if not exists mc_delete_portaria delete on portaria referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'portaria',
            'del',
            velho.cod_unidade||'|'||velho.cod_portaria,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- profissoes
---

database minas;

create trigger if not exists mc_insert_profissoes insert on profissoes referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'profissoes',
            'ins',
            novo.cod_profissao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_profissoes;

create trigger if not exists mc_insert_profissoes insert on profissoes referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'profissoes',
            'ins',
            novo.cod_profissao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_profissoes;

create trigger if not exists mc_update_profissoes update on profissoes referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'profissoes',
            'upd',
            novo.cod_profissao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_profissoes;

create trigger  if not exists mc_delete_profissoes delete on profissoes referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'profissoes',
            'del',
            velho.cod_profissao,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- rec_nau_minas
---

database nautico;

create trigger if not exists mc_insert_rec_nau_minas insert on rec_nau_minas referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'rec_nau_minas',
            'ins',
            novo.idf_naumin,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_rec_nau_minas;

create trigger if not exists mc_insert_rec_nau_minas insert on rec_nau_minas referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'rec_nau_minas',
            'ins',
            novo.idf_naumin,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_rec_nau_minas;

create trigger if not exists mc_update_rec_nau_minas update on rec_nau_minas referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'rec_nau_minas',
            'upd',
            novo.idf_naumin,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_rec_nau_minas;

create trigger  if not exists mc_delete_rec_nau_minas delete on rec_nau_minas referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'rec_nau_minas',
            'del',
            velho.idf_naumin,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- receita
---

database minas;

create trigger if not exists mc_insert_receita insert on receita referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'receita',
            'ins',
            novo.cod_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_receita;

create trigger if not exists mc_insert_receita insert on receita referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'receita',
            'ins',
            novo.cod_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_receita;

create trigger if not exists mc_update_receita update on receita referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'receita',
            'upd',
            novo.cod_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_receita;

create trigger  if not exists mc_delete_receita delete on receita referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'receita',
            'del',
            velho.cod_receita,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- taxa_parentesco
---

database minas;

create trigger if not exists mc_insert_taxa_parentesco insert on taxa_parentesco referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'taxa_parentesco',
            'ins',
            novo.cod_parentesco,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_taxa_parentesco;

create trigger if not exists mc_insert_taxa_parentesco insert on taxa_parentesco referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'taxa_parentesco',
            'ins',
            novo.cod_parentesco,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_taxa_parentesco;

create trigger if not exists mc_update_taxa_parentesco update on taxa_parentesco referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'taxa_parentesco',
            'upd',
            novo.cod_parentesco,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_taxa_parentesco;

create trigger  if not exists mc_delete_taxa_parentesco delete on taxa_parentesco referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'taxa_parentesco',
            'del',
            velho.cod_parentesco,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_concessao
---

database minas;

create trigger if not exists mc_insert_tipo_concessao insert on tipo_concessao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_concessao',
            'ins',
            novo.cod_tipo_concessao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_concessao;

create trigger if not exists mc_insert_tipo_concessao insert on tipo_concessao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_concessao',
            'ins',
            novo.cod_tipo_concessao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_concessao;

create trigger if not exists mc_update_tipo_concessao update on tipo_concessao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_concessao',
            'upd',
            novo.cod_tipo_concessao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_concessao;

create trigger  if not exists mc_delete_tipo_concessao delete on tipo_concessao referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_concessao',
            'del',
            velho.cod_tipo_concessao,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_contribuinte
---

database minas;

create trigger if not exists mc_insert_tipo_contribuinte insert on tipo_contribuinte referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_contribuinte',
            'ins',
            novo.cod_tipo_contrib,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_contribuinte;

create trigger if not exists mc_insert_tipo_contribuinte insert on tipo_contribuinte referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_contribuinte',
            'ins',
            novo.cod_tipo_contrib,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_contribuinte;

create trigger if not exists mc_update_tipo_contribuinte update on tipo_contribuinte referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_contribuinte',
            'upd',
            novo.cod_tipo_contrib,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_contribuinte;

create trigger  if not exists mc_delete_tipo_contribuinte delete on tipo_contribuinte referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_contribuinte',
            'del',
            velho.cod_tipo_contrib,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_documento
---

database minas;

create trigger if not exists mc_insert_tipo_documento insert on tipo_documento referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_documento',
            'ins',
            novo.idf_tipo_documento,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_documento;

create trigger if not exists mc_insert_tipo_documento insert on tipo_documento referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_documento',
            'ins',
            novo.idf_tipo_documento,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_documento;

create trigger if not exists mc_update_tipo_documento update on tipo_documento referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_documento',
            'upd',
            novo.idf_tipo_documento,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_documento;

create trigger  if not exists mc_delete_tipo_documento delete on tipo_documento referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_documento',
            'del',
            velho.idf_tipo_documento,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_receita
---

database minas;

create trigger if not exists mc_insert_tipo_receita insert on tipo_receita referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_receita',
            'ins',
            novo.cod_tipo_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_receita;

create trigger if not exists mc_insert_tipo_receita insert on tipo_receita referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_receita',
            'ins',
            novo.cod_tipo_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_receita;

create trigger if not exists mc_update_tipo_receita update on tipo_receita referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_receita',
            'upd',
            novo.cod_tipo_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_receita;

create trigger  if not exists mc_delete_tipo_receita delete on tipo_receita referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_receita',
            'del',
            velho.cod_tipo_receita,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- uf
---

database minasdba;

create trigger if not exists mc_insert_uf insert on uf referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'uf',
            'ins',
            novo.cod_uf,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_uf;

create trigger if not exists mc_insert_uf insert on uf referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'uf',
            'ins',
            novo.cod_uf,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_uf;

create trigger if not exists mc_update_uf update on uf referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'uf',
            'upd',
            novo.cod_uf,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_uf;

create trigger  if not exists mc_delete_uf delete on uf referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'uf',
            'del',
            velho.cod_uf,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- unidade
---

database minas;

create trigger if not exists mc_insert_unidade insert on unidade referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'unidade',
            'ins',
            novo.cod_unidade,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_unidade;

create trigger if not exists mc_insert_unidade insert on unidade referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'unidade',
            'ins',
            novo.cod_unidade,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_unidade;

create trigger if not exists mc_update_unidade update on unidade referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'unidade',
            'upd',
            novo.cod_unidade,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_unidade;

create trigger  if not exists mc_delete_unidade delete on unidade referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'unidade',
            'del',
            velho.cod_unidade,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- acerto_cancel_rec
---

database minas;

create trigger if not exists mc_insert_acerto_cancel_rec insert on acerto_cancel_rec referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acerto_cancel_rec',
            'ins',
            novo.nro_acerto_cancel||'|'||novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_associado||'|'||novo.cod_receita||'|'||novo.dat_receita||'|'||novo.hor_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_acerto_cancel_rec;

create trigger if not exists mc_insert_acerto_cancel_rec insert on acerto_cancel_rec referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acerto_cancel_rec',
            'ins',
            novo.nro_acerto_cancel||'|'||novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_associado||'|'||novo.cod_receita||'|'||novo.dat_receita||'|'||novo.hor_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_acerto_cancel_rec;

create trigger if not exists mc_update_acerto_cancel_rec update on acerto_cancel_rec referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acerto_cancel_rec',
            'upd',
            novo.nro_acerto_cancel||'|'||novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_associado||'|'||novo.cod_receita||'|'||novo.dat_receita||'|'||novo.hor_receita,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_acerto_cancel_rec;

create trigger  if not exists mc_delete_acerto_cancel_rec delete on acerto_cancel_rec referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acerto_cancel_rec',
            'del',
            velho.nro_acerto_cancel||'|'||velho.cod_tipo_associado||'|'||velho.cod_cota||'|'||velho.cod_associado||'|'||velho.cod_receita||'|'||velho.dat_receita||'|'||velho.hor_receita,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_cota
---

database minas;

create trigger if not exists mc_insert_tipo_cota insert on tipo_cota referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_cota',
            'ins',
            novo.cod_tipo_cota,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_cota;

create trigger if not exists mc_insert_tipo_cota insert on tipo_cota referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_cota',
            'ins',
            novo.cod_tipo_cota,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_cota;

create trigger if not exists mc_update_tipo_cota update on tipo_cota referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_cota',
            'upd',
            novo.cod_tipo_cota,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_cota;

create trigger  if not exists mc_delete_tipo_cota delete on tipo_cota referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_cota',
            'del',
            velho.cod_tipo_cota,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- modalidade
---

database minas;

create trigger if not exists mc_insert_modalidade insert on modalidade referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'modalidade',
            'ins',
            novo.cod_mod_curso,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_modalidade;

create trigger if not exists mc_insert_modalidade insert on modalidade referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'modalidade',
            'ins',
            novo.cod_mod_curso,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_modalidade;

create trigger if not exists mc_update_modalidade update on modalidade referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'modalidade',
            'upd',
            novo.cod_mod_curso,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_modalidade;

create trigger  if not exists mc_delete_modalidade delete on modalidade referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'modalidade',
            'del',
            velho.cod_mod_curso,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- curso
---

database minas;

create trigger if not exists mc_insert_curso insert on curso referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'curso',
            'ins',
            novo.cod_curso,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_curso;

create trigger if not exists mc_insert_curso insert on curso referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'curso',
            'ins',
            novo.cod_curso,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_curso;

create trigger if not exists mc_update_curso update on curso referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'curso',
            'upd',
            novo.cod_curso,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_curso;

create trigger  if not exists mc_delete_curso delete on curso referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'curso',
            'del',
            velho.cod_curso,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- nivel
---

database minas;

create trigger if not exists mc_insert_nivel insert on nivel referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'nivel',
            'ins',
            novo.cod_nivel,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_nivel;

create trigger if not exists mc_insert_nivel insert on nivel referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'nivel',
            'ins',
            novo.cod_nivel,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_nivel;

create trigger if not exists mc_update_nivel update on nivel referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'nivel',
            'upd',
            novo.cod_nivel,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_nivel;

create trigger  if not exists mc_delete_nivel delete on nivel referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'nivel',
            'del',
            velho.cod_nivel,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- turma
---

database minas;

create trigger if not exists mc_insert_turma insert on turma referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'turma',
            'ins',
            novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_turma;

create trigger if not exists mc_insert_turma insert on turma referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'turma',
            'ins',
            novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_turma;

create trigger if not exists mc_update_turma update on turma referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'turma',
            'upd',
            novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_turma;

create trigger  if not exists mc_delete_turma delete on turma referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'turma',
            'del',
            velho.cod_curso||'|'||velho.cod_turma,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- dias_let_curso
---

database minas;

create trigger if not exists mc_insert_dias_let_curso insert on dias_let_curso referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'dias_let_curso',
            'ins',
            novo.cod_dias_let||'|'||novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_dias_let_curso;

create trigger if not exists mc_insert_dias_let_curso insert on dias_let_curso referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'dias_let_curso',
            'ins',
            novo.cod_dias_let||'|'||novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_dias_let_curso;

create trigger if not exists mc_update_dias_let_curso update on dias_let_curso referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'dias_let_curso',
            'upd',
            novo.cod_dias_let||'|'||novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_dias_let_curso;

create trigger  if not exists mc_delete_dias_let_curso delete on dias_let_curso referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'dias_let_curso',
            'del',
            velho.cod_dias_let||'|'||velho.cod_curso||'|'||velho.cod_turma,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- atestado
---

database minas;

create trigger if not exists mc_insert_atestado insert on atestado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'atestado',
            'ins',
            novo.cod_atestado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_atestado;

create trigger if not exists mc_insert_atestado insert on atestado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'atestado',
            'ins',
            novo.cod_atestado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_atestado;

create trigger if not exists mc_update_atestado update on atestado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'atestado',
            'upd',
            novo.cod_atestado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_atestado;

create trigger  if not exists mc_delete_atestado delete on atestado referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'atestado',
            'del',
            velho.cod_atestado,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- associado_atestado
---

database minas;

create trigger if not exists mc_insert_associado_atestado insert on associado_atestado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_atestado',
            'ins',
            novo.nro_seq_ateass,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_associado_atestado;

create trigger if not exists mc_insert_associado_atestado insert on associado_atestado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_atestado',
            'ins',
            novo.nro_seq_ateass,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_associado_atestado;

create trigger if not exists mc_update_associado_atestado update on associado_atestado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_atestado',
            'upd',
            novo.nro_seq_ateass,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_associado_atestado;

create trigger  if not exists mc_delete_associado_atestado delete on associado_atestado referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_atestado',
            'del',
            velho.nro_seq_ateass,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- aluno
---

database minas;

create trigger if not exists mc_insert_aluno insert on aluno referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'aluno',
            'ins',
            novo.cod_associado||'|'||novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_aluno;

create trigger if not exists mc_insert_aluno insert on aluno referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'aluno',
            'ins',
            novo.cod_associado||'|'||novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_aluno;

create trigger if not exists mc_update_aluno update on aluno referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'aluno',
            'upd',
            novo.cod_associado||'|'||novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_aluno;

create trigger  if not exists mc_delete_aluno delete on aluno referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'aluno',
            'del',
            velho.cod_associado||'|'||velho.cod_curso||'|'||velho.cod_turma,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tmp_local
---

database minas;

create trigger if not exists mc_insert_tmp_local insert on tmp_local referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tmp_local',
            'ins',
            novo.cod_unidade||'|'||novo.nro_seq_local,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tmp_local;

create trigger if not exists mc_insert_tmp_local insert on tmp_local referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tmp_local',
            'ins',
            novo.cod_unidade||'|'||novo.nro_seq_local,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tmp_local;

create trigger if not exists mc_update_tmp_local update on tmp_local referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tmp_local',
            'upd',
            novo.cod_unidade||'|'||novo.nro_seq_local,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tmp_local;

create trigger  if not exists mc_delete_tmp_local delete on tmp_local referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tmp_local',
            'del',
            velho.cod_unidade||'|'||velho.nro_seq_local,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- cota_taxa_freq
---

database minas;

create trigger if not exists mc_insert_cota_taxa_freq insert on cota_taxa_freq referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_taxa_freq',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_associado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_cota_taxa_freq;

create trigger if not exists mc_insert_cota_taxa_freq insert on cota_taxa_freq referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_taxa_freq',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_associado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_cota_taxa_freq;

create trigger if not exists mc_update_cota_taxa_freq update on cota_taxa_freq referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_taxa_freq',
            'upd',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.cod_associado,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_cota_taxa_freq;

create trigger  if not exists mc_delete_cota_taxa_freq delete on cota_taxa_freq referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_taxa_freq',
            'del',
            velho.cod_tipo_associado||'|'||velho.cod_cota||'|'||velho.cod_associado,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_isencao
---

database minas;

create trigger if not exists mc_insert_tipo_isencao insert on tipo_isencao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_isencao',
            'ins',
            novo.cod_tipo_isencao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_isencao;

create trigger if not exists mc_insert_tipo_isencao insert on tipo_isencao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_isencao',
            'ins',
            novo.cod_tipo_isencao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_isencao;

create trigger if not exists mc_update_tipo_isencao update on tipo_isencao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_isencao',
            'upd',
            novo.cod_tipo_isencao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_isencao;

create trigger  if not exists mc_delete_tipo_isencao delete on tipo_isencao referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_isencao',
            'del',
            velho.cod_tipo_isencao,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_isencao
---

database minas;

create trigger if not exists mc_insert_tipo_isencao insert on tipo_isencao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_isencao',
            'ins',
            novo.cod_tipo_isencao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_isencao;

create trigger if not exists mc_insert_tipo_isencao insert on tipo_isencao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_isencao',
            'ins',
            novo.cod_tipo_isencao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_isencao;

create trigger if not exists mc_update_tipo_isencao update on tipo_isencao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_isencao',
            'upd',
            novo.cod_tipo_isencao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_isencao;

create trigger  if not exists mc_delete_tipo_isencao delete on tipo_isencao referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_isencao',
            'del',
            velho.cod_tipo_isencao,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_acompanhante
---

database minas;

create trigger if not exists mc_insert_tipo_acompanhante insert on tipo_acompanhante referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_acompanhante',
            'ins',
            novo.cod_tipo_acomp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_acompanhante;

create trigger if not exists mc_insert_tipo_acompanhante insert on tipo_acompanhante referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_acompanhante',
            'ins',
            novo.cod_tipo_acomp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_acompanhante;

create trigger if not exists mc_update_tipo_acompanhante update on tipo_acompanhante referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_acompanhante',
            'upd',
            novo.cod_tipo_acomp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_acompanhante;

create trigger  if not exists mc_delete_tipo_acompanhante delete on tipo_acompanhante referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_acompanhante',
            'del',
            velho.cod_tipo_acomp,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- acomp_associado
---

database minas;

create trigger if not exists mc_insert_acomp_associado insert on acomp_associado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acomp_associado',
            'ins',
            novo.cod_associado||'|'||novo.nro_seq_acomp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_acomp_associado;

create trigger if not exists mc_insert_acomp_associado insert on acomp_associado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acomp_associado',
            'ins',
            novo.cod_associado||'|'||novo.nro_seq_acomp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_acomp_associado;

create trigger if not exists mc_update_acomp_associado update on acomp_associado referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acomp_associado',
            'upd',
            novo.cod_associado||'|'||novo.nro_seq_acomp,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_acomp_associado;

create trigger  if not exists mc_delete_acomp_associado delete on acomp_associado referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acomp_associado',
            'del',
            velho.cod_associado||'|'||velho.nro_seq_acomp,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- mot_liberacao
---

database minas;

create trigger if not exists mc_insert_mot_liberacao insert on mot_liberacao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'mot_liberacao',
            'ins',
            novo.cod_mot_liberacao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_mot_liberacao;

create trigger if not exists mc_insert_mot_liberacao insert on mot_liberacao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'mot_liberacao',
            'ins',
            novo.cod_mot_liberacao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_mot_liberacao;

create trigger if not exists mc_update_mot_liberacao update on mot_liberacao referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'mot_liberacao',
            'upd',
            novo.cod_mot_liberacao,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_mot_liberacao;

create trigger  if not exists mc_delete_mot_liberacao delete on mot_liberacao referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'mot_liberacao',
            'del',
            velho.cod_mot_liberacao,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_desconto
---

database minas;

create trigger if not exists mc_insert_tipo_desconto insert on tipo_desconto referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_desconto',
            'ins',
            novo.cod_tipo_desconto,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_desconto;

create trigger if not exists mc_insert_tipo_desconto insert on tipo_desconto referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_desconto',
            'ins',
            novo.cod_tipo_desconto,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_desconto;

create trigger if not exists mc_update_tipo_desconto update on tipo_desconto referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_desconto',
            'upd',
            novo.cod_tipo_desconto,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_desconto;

create trigger  if not exists mc_delete_tipo_desconto delete on tipo_desconto referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_desconto',
            'del',
            velho.cod_tipo_desconto,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- cota_pagto_rejeit
---

database minas;

create trigger if not exists mc_insert_cota_pagto_rejeit insert on cota_pagto_rejeit referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_pagto_rejeit',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.nro_fatura||'|'||novo.dat_pagto,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_cota_pagto_rejeit;

create trigger if not exists mc_insert_cota_pagto_rejeit insert on cota_pagto_rejeit referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_pagto_rejeit',
            'ins',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.nro_fatura||'|'||novo.dat_pagto,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_cota_pagto_rejeit;

create trigger if not exists mc_update_cota_pagto_rejeit update on cota_pagto_rejeit referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_pagto_rejeit',
            'upd',
            novo.cod_tipo_associado||'|'||novo.cod_cota||'|'||novo.nro_fatura||'|'||novo.dat_pagto,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_cota_pagto_rejeit;

create trigger  if not exists mc_delete_cota_pagto_rejeit delete on cota_pagto_rejeit referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'cota_pagto_rejeit',
            'del',
            velho.cod_tipo_associado||'|'||velho.cod_cota||'|'||velho.nro_fatura||'|'||velho.dat_pagto,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- plano_receita
---

database minas;

create trigger if not exists mc_insert_plano_receita insert on plano_receita referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'plano_receita',
            'ins',
            novo.cod_receita||'|'||novo.cod_plano,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_plano_receita;

create trigger if not exists mc_insert_plano_receita insert on plano_receita referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'plano_receita',
            'ins',
            novo.cod_receita||'|'||novo.cod_plano,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_plano_receita;

create trigger if not exists mc_update_plano_receita update on plano_receita referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'plano_receita',
            'upd',
            novo.cod_receita||'|'||novo.cod_plano,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_plano_receita;

create trigger  if not exists mc_delete_plano_receita delete on plano_receita referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'plano_receita',
            'del',
            velho.cod_receita||'|'||velho.cod_plano,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- usuario
---

database acesso;

create trigger if not exists mc_insert_usuario insert on usuario referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'usuario',
            'ins',
            novo.cod_usuario,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_usuario;

create trigger if not exists mc_insert_usuario insert on usuario referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'usuario',
            'ins',
            novo.cod_usuario,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_usuario;

create trigger if not exists mc_update_usuario update on usuario referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'usuario',
            'upd',
            novo.cod_usuario,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_usuario;

create trigger  if not exists mc_delete_usuario delete on usuario referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'usuario',
            'del',
            velho.cod_usuario,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- horario_pessoa
---

database minas;

create trigger if not exists mc_insert_horario_pessoa insert on horario_pessoa referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'horario_pessoa',
            'ins',
            novo.idf_horario_pessoa,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_horario_pessoa;

create trigger if not exists mc_insert_horario_pessoa insert on horario_pessoa referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'horario_pessoa',
            'ins',
            novo.idf_horario_pessoa,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_horario_pessoa;

create trigger if not exists mc_update_horario_pessoa update on horario_pessoa referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'horario_pessoa',
            'upd',
            novo.idf_horario_pessoa,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_horario_pessoa;

create trigger  if not exists mc_delete_horario_pessoa delete on horario_pessoa referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'horario_pessoa',
            'del',
            velho.idf_horario_pessoa,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- acerto_cancel
---

database minas;

create trigger if not exists mc_insert_acerto_cancel insert on acerto_cancel referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acerto_cancel',
            'ins',
            novo.nro_acerto_cancel,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_acerto_cancel;

create trigger if not exists mc_insert_acerto_cancel insert on acerto_cancel referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acerto_cancel',
            'ins',
            novo.nro_acerto_cancel,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_acerto_cancel;

create trigger if not exists mc_update_acerto_cancel update on acerto_cancel referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acerto_cancel',
            'upd',
            novo.nro_acerto_cancel,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_acerto_cancel;

create trigger  if not exists mc_delete_acerto_cancel delete on acerto_cancel referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'acerto_cancel',
            'del',
            velho.nro_acerto_cancel,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- aluno_licenca
---

database minas;

create trigger if not exists mc_insert_aluno_licenca insert on aluno_licenca referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'aluno_licenca',
            'ins',
            novo.cod_associado||'|'||novo.cod_curso||'|'||novo.cod_turma||'|'||novo.dat_inicio_licenca,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_aluno_licenca;

create trigger if not exists mc_insert_aluno_licenca insert on aluno_licenca referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'aluno_licenca',
            'ins',
            novo.cod_associado||'|'||novo.cod_curso||'|'||novo.cod_turma||'|'||novo.dat_inicio_licenca,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_aluno_licenca;

create trigger if not exists mc_update_aluno_licenca update on aluno_licenca referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'aluno_licenca',
            'upd',
            novo.cod_associado||'|'||novo.cod_curso||'|'||novo.cod_turma||'|'||novo.dat_inicio_licenca,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_aluno_licenca;

create trigger  if not exists mc_delete_aluno_licenca delete on aluno_licenca referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'aluno_licenca',
            'del',
            velho.cod_associado||'|'||velho.cod_curso||'|'||velho.cod_turma||'|'||velho.dat_inicio_licenca,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- pessoa_fisica
---

database minas;

create trigger if not exists mc_insert_pessoa_fisica insert on pessoa_fisica referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'pessoa_fisica',
            'ins',
            novo.idt_pessoa||'|'||novo.cod_pessoa,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_pessoa_fisica;

create trigger if not exists mc_insert_pessoa_fisica insert on pessoa_fisica referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'pessoa_fisica',
            'ins',
            novo.idt_pessoa||'|'||novo.cod_pessoa,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_pessoa_fisica;

create trigger if not exists mc_update_pessoa_fisica update on pessoa_fisica referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'pessoa_fisica',
            'upd',
            novo.idt_pessoa||'|'||novo.cod_pessoa,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_pessoa_fisica;

create trigger  if not exists mc_delete_pessoa_fisica delete on pessoa_fisica referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'pessoa_fisica',
            'del',
            velho.idt_pessoa||'|'||velho.cod_pessoa,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- associado_servico
---

database minas;

create trigger if not exists mc_insert_associado_servico insert on associado_servico referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_servico',
            'ins',
            novo.idf_associado_servico,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_associado_servico;

create trigger if not exists mc_insert_associado_servico insert on associado_servico referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_servico',
            'ins',
            novo.idf_associado_servico,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_associado_servico;

create trigger if not exists mc_update_associado_servico update on associado_servico referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_servico',
            'upd',
            novo.idf_associado_servico,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_associado_servico;

create trigger  if not exists mc_delete_associado_servico delete on associado_servico referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'associado_servico',
            'del',
            velho.idf_associado_servico,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- tipo_servico
---

database minas;

create trigger if not exists mc_insert_tipo_servico insert on tipo_servico referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_servico',
            'ins',
            novo.idf_tipo_servico,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_tipo_servico;

create trigger if not exists mc_insert_tipo_servico insert on tipo_servico referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_servico',
            'ins',
            novo.idf_tipo_servico,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_tipo_servico;

create trigger if not exists mc_update_tipo_servico update on tipo_servico referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_servico',
            'upd',
            novo.idf_tipo_servico,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_tipo_servico;

create trigger  if not exists mc_delete_tipo_servico delete on tipo_servico referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'tipo_servico',
            'del',
            velho.idf_tipo_servico,
            prioridade_sincronismo()
        )
    );


        ----------------------------------------------------------------------------------------
--- fila_espera
---

database minas;

create trigger if not exists mc_insert_fila_espera insert on fila_espera referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'fila_espera',
            'ins',
            novo.cod_associado||'|'||novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_fila_espera;

create trigger if not exists mc_insert_fila_espera insert on fila_espera referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'fila_espera',
            'ins',
            novo.cod_associado||'|'||novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_fila_espera;

create trigger if not exists mc_update_fila_espera update on fila_espera referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'fila_espera',
            'upd',
            novo.cod_associado||'|'||novo.cod_curso||'|'||novo.cod_turma,
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_fila_espera;

create trigger  if not exists mc_delete_fila_espera delete on fila_espera referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            'fila_espera',
            'del',
            velho.cod_associado||'|'||velho.cod_curso||'|'||velho.cod_turma,
            prioridade_sincronismo()
        )
    );


        