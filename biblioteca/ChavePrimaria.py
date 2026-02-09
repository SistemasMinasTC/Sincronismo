#!/usr/bin/python
#

import os, time, logging, sqlite3

from pywebio.input import *
from pywebio.output import *
from pywebio.pin import *
from pywebio_battery import popup_input
from pywebio_battery import confirm
from pywebio.session import *
from biblioteca.conexoes import *

from recordtype import recordtype
from biblioteca.createSincronismo import nomeTabelas, nomeColunas

class CrudChavePrimaria():
    def __init__(self):
        self.sqlite = sqlite3.connect('ParametrosMinasCorp.db', check_same_thread=False)
        self.cr = self.sqlite.cursor()
        self.cr.execute("""
            create table if not exists ChavePrimaria
            (
                IdChavePrimaria integer primary key autoincrement,
                Banco varchar(20) not null,
                Tabela varchar(100) not null,
                Colunas varchar(255) not null
            )
        """)
        self.sqlite.commit()

        self.lista()

    def lista(self):
        self.cr.execute("""
            select
                IdChavePrimaria,
                Banco,
                Tabela,
                Colunas
            from ChavePrimaria
            order by
                Banco,
                Tabela
        """)

        cabecalho = [col[0] for col in self.cr.description]
        dados = [dict(zip(cabecalho,linha)) for linha in self.cr]

        with use_scope('form', clear=True):
            put_html('<h1><center>Cadastro de Chaves Primarias</center></h1>')
            put_buttons(('Incluir','Gerar Triggers'),onclick = (self.dialogo,self.gerarTriggers))
            put_datatable(
                dados,
                column_order = [c for c in cabecalho if c != 'IdChavePrimaria'],
                id_field = 'IdChavePrimaria',
                instance_id = 'listaChavePrimaria',
                cell_content_bar = False,
                onselect = self.dialogo,
                column_args = {
                    'Banco': {'flex': 10},
                    'Tabela': {'flex': 10},
                    'Colunas': {'flex': 80},
                },
                theme = 'alpine',
            )

    def dialogo(self, Id = None):
        def excluir(Id):
            if confirm('Excluir ChavePrimaria', f'Confirma a exclusão da ChavePrimaria {Id}?'):
                self.cr.execute('''
                    delete from ChavePrimaria
                    where
                        IdChavePrimaria = ?
                    ''',
                    (
                        (Id,)
                    )
                )

                self.sqlite.commit()

                datatable_remove(
                    'listaChavePrimaria',
                    row_ids = (Id,)
                )

        self.cr.execute('''
            select
                IdChavePrimaria,
                Banco,
                Tabela,
                Colunas
            from ChavePrimaria
            where
               IdChavePrimaria = ?
        ''', (Id,))

        colunas = [col[0] for col in self.cr.description]
        linha = self.cr.fetchone()
        linha = dict(zip(colunas, linha if linha else [None for c in colunas]))


        pin_on_change('Banco', lambda buscaNomeTabelas: pin_update('Tabela', options=nomeTabelas('Informix', pin.Banco),value=buscaNomeTabelas))
        pin_on_change('Tabela', lambda buscaNomeColunas: pin_update('Colunas', options=nomeColunas('Informix', f'select * from {buscaNomeColunas}',banco=pin.Banco)))

        registro = popup_input(
            [
                put_button('Excluir', onclick = lambda: excluir(Id)),
                put_select (label="Banco de dados", name='Banco', options=('minas','nautico','acesso','foto_minas','minasdba'),value='minas'),
                put_select(label="Tabela", name='Tabela', options=nomeTabelas('Informix', linha['Banco'] if linha['Banco'] else 'minas'),value=linha['Tabela'] if linha['Tabela'] else '_cota_'),
                put_select(label="Chave primária", name='Colunas', options=[[n,n,n in linha['Colunas'].split(', ') if linha['Colunas'] else []] for n in (nomeColunas('Informix', f"select * from {linha['Tabela'] if linha['Tabela'] else '_cota_'}"))], multiple=True).style('width: 600px;'),
            ],
            title = 'ChavePrimaria',
            popup_size = 'large',
            cancelable = True
        )

        if registro:
            if Id:
                self.cr.execute('''
                    update ChavePrimaria set
                        Banco = ?,
                        Tabela = ?,
                        Colunas = ?
                    where
                        IdChavePrimaria = ?
                    ''',
                    (
                        registro['Banco'],
                        registro['Tabela'],
                        ', '.join(registro['Colunas']),
                        linha['IdChavePrimaria'],
                    )
                )
                self.sqlite.commit()

                registro['Colunas'] = ', '.join(registro['Colunas'])

                datatable_update(
                    'listaChavePrimaria',
                    data = registro,
                    row_id = Id,
                )
            else:
                self.cr.execute('''
                    insert into ChavePrimaria
                    (
                        Banco,
                        Tabela,
                        Colunas
                    )
                    values
                    (
                        ? /* Banco */,
                        ? /* Tabela */,
                        ? /* Colunas*/
                    )
                    returning IdChavePrimaria
                    ''',
                    (
                        registro['Banco'],
                        registro['Tabela'],
                        ', '.join(registro['Colunas']),
                    )
                )

                Id = self.cr.fetchone()[0]
                self.sqlite.commit()

                registro['IdChavePrimaria'] = Id
                registro['Colunas'] = ', '.join(registro['Colunas'])

                datatable_insert(
                    'listaChavePrimaria',
                    records = registro,
                    row_id = Id,
                )

    def gerarTriggers(self):
        arquivo = open(f'biblioteca/create_triggers.sql','w')
        self.cr.execute("""
            select
                Banco,
                Tabela,
                Colunas
            from ChavePrimaria
        """)

        Linha = recordtype('Linha',[col[0] for col in self.cr.description])

        for linha in [Linha(*l) for l in self.cr]:
            # Cria os triggers
            #
            linha.Colunas = linha.Colunas.split(', ')

            arquivo.write(f"""\
----------------------------------------------------------------------------------------
--- {linha.Tabela}
---

database {linha.Banco};

create trigger if not exists mc_insert_{linha.Tabela} insert on {linha.Tabela} referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '{linha.Tabela}',
            'ins',
            novo.{"||'|'||novo.".join(linha.Colunas)},
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_insert_{linha.Tabela};

create trigger if not exists mc_insert_{linha.Tabela} insert on {linha.Tabela} referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '{linha.Tabela}',
            'ins',
            novo.{"||'|'||novo.".join(linha.Colunas)},
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_update_{linha.Tabela};

create trigger if not exists mc_update_{linha.Tabela} update on {linha.Tabela} referencing new as novo
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '{linha.Tabela}',
            'upd',
            novo.{"||'|'||novo.".join(linha.Colunas)},
            prioridade_sincronismo()
        )
    );

drop trigger if exists mc_delete_{linha.Tabela};

create trigger  if not exists mc_delete_{linha.Tabela} delete on {linha.Tabela} referencing old as velho
for each row
    when (not sincronizando())
    (
        insert into mc_log(data_hora,banco,tabela,operacao,pk,tentativas)
        values
        (
            current,
            dbinfo('dbname'),
            '{linha.Tabela}',
            'del',
            velho.{"||'|'||velho.".join(linha.Colunas)},
            prioridade_sincronismo()
        )
    );


        """)

        toast('Script gerado')

# Teste
#
if __name__ == "__main__":
    crud = CrudChavePrimaria()

