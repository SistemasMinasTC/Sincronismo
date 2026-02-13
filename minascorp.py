#!/usr/bin/python
#
import os
import logging
from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s:%(funcName)s %(levelname)s: %(message)s",
    handlers=[
        RotatingFileHandler(
            filename="/var/log/minas_corporativo.log" if os.getenv('AMBIENTE_NSS') == 'PRODUCAO' else "/tmp/minas_corporativo.log",
            encoding="utf-8",
            maxBytes=0x500000,
            backupCount=3
        )
    ]
)

from pywebio import start_server
from biblioteca.sincroniza import Sincroniza
from front.interface import Interface

# Instâncias do sincronizador

sincronizador = {}
sincronizador['geral'] = Sincroniza (
    'geral',                    # Nome
    #
    # informix -> mssql
    #
    '_cota_',                   # Cota
    '_movimentacao_receita_',   # ReceitaCota
    'acerto_cancel_rec',        # ReceitaCota (update)
    'acerto_cancel',            # Acerto
    'acompanhante',             # Acompanhante
    'acomp_associado',          # AcompanhanteAssociado
    'agregado',                 # Adesao
    'aluno',                    # Aluno
    'aluno_licenca',            # LicencaMedica
    'associado',                # Pessoa
    'associado_excl',           # Pessoa
    'associado_atestado',       # PessoaAtestadoMedico
    'atestado',                 # AtestadoMedico
    'banco',                    # Banco
    'cota_associado',           # Associado
    'cota_debito_auto',         # DebitoAutomatico
    'cota_pagto_rejeit',        # PagamentoRejeitado
    'cota_taxa_freq',           # Taxa Frequencia
    'cidadao',                  # Usuarios
    'curso',                    # Curso
    'dias_let_curso',           # Inclui a turma no mc_log
    #'disp_entrada_saida',      # DipositivoAcesso
    'fatura',                   # Fatura
    'horario_pessoa',           # HorarioAcompanhante
    'item_fat_eterno',          # ItemFatura
    'modalidade',               # ModalidadeEsportiva
    'mot_liberacao',            # MotivoLiberacao
    'mot_movto',                # Motivo
    'nacionalidade',            # Nacionalidade
    'nivel',                    # Nivel
    'parcela_plano',            # ParcelaPlanoCobranca
    'ped_transf',               # PedidoTransferencia
    'pessoa_fisica',            # Update em cota_associado
    'plano_cobranca',           # PlanoCobranca
    'plano_receita',            # ReceitaPlanoCobranca
    #'portaria',                # Portaria
    'profissoes',               # Profissao
    'receita',                  # Receita
    'rec_nau_minas',            # EquivalenciaReceita
    'reserva_local',            # GE_LocalReserva
    'taxa_parentesco',          # Vinculo e TaxaDependente
    'tipo_acompanhante',        # TipoAcompanhante
    'tipo_concessao',           # ClasseCota
    'tipo_contribuinte',        # ClasseCota
    'tipo_cota',                # ClasseCota
    'tipo_desconto',            # ClasseCota
    'tipo_documento',           # TipoDocumento
    'tipo_isencao',             # TipoIsencao
    'tipo_receita',             # TipoReceita
    'tmp_local',                # Local
    'turma',                    # Turma
    'uf',                       # UF
    'unidade',                  # Unidade
    'usuario',                  # usuario
    #
    # mssql -> informix
    #
    'Acerto',                   # acerto e acerto_cancel_rec
    'DebitoAutomatico',         # cota_debito_auto
    'Nacionalidade',            # nacionalidade
    'TipoDocumento',            # tipo_documento
    'ReceitaCota',              # receita_cota, posicao_cota, receita_assoc_excl, posicao_assoc_excl
    'Fatura',                   # fatura
    'ItemFatura'                # item_fatura, item_fat_eterno, item_fat_receita
)
sincronizador['geral'].start()

sincronizador['portaria'] = Sincroniza (
    'portaria',                 # Nome
    'LogAcesso',                # log_movto_portaria/log_movto_local
)
sincronizador['portaria'].start()


def main():
    interface = Interface(sincronizador)

if __name__ == "__main__":
    logging.info('Início do processamento')

    start_server(
        main,
        port=1972 if os.getenv('AMBIENTE_NSS') != 'PRODUCAO' else 1958,
        debug=True,
        static_dir='front/static'
    )

    logging.info('Fim do processamento')
