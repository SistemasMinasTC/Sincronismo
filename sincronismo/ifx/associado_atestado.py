#!/usr/bin/python
#

import os
import sys
from recordtype import recordtype
from google.cloud import storage

def convert(conn_ifx, conn_sql, linha_log):
    credencial="/usr/local/etc/CredenciaisMinasTenis/CredencialGCP.json" if os.name == 'posix' else "C:\\CredenciaisMinasTenis\\CredencialGCP.json"
    storage_client = storage.Client.from_service_account_json(credencial)
    bucket = storage_client.bucket('minascorp')

    cr_sql = conn_sql.cursor()
    try:
        cr_sql.execute('create table #sincronizando (dummy char(1))')
    except:
        pass

    cr_ifx = conn_ifx.cursor()
    cr_ifx.execute('execute procedure em_sincronismo()')

    Chave = recordtype('Chave', 'nro_seq_ateass')
    chave = Chave(*linha_log.pk.split('|'))

    if linha_log.operacao == 'del':
        cr_sql.execute(f"""
            delete from PessoaAtestadoMedico
            where
                IdPessoaAtestado = (select PkSql from PkDePara where Tabela = 'PessoaAtestadoMedico' and PkIfx = ?)
        """, (
            linha_log.pk
        ))

        cr_sql.close()
        return

    cr_ifx.execute(f"""
        select
            'Associado|' || cod_associado as cod_associado,
            cod_atestado,
            dat_atestado,
            dat_inclusao,
            mla_validador,
            dat_validacao,
            img_atestado
        from {linha_log.banco}:associado_atestado as associado_atestado
        where
            nro_seq_ateass = ?
    """,(
        chave.nro_seq_ateass,
    ))

    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])
    linha = cr_ifx.fetchone()
    origem = Linha(*linha) if linha else None

    cr_sql.execute("""select PkSql from PkDePara where Tabela = 'PessoaAtestadoMedico' and PkIfx = ?""", (linha_log.pk,))
    IdPessoaAtestadoMedico = cr_sql.fetchval()

    cr_sql.execute("""
        update PessoaAtestadoMedico set
            IdPessoa = (select PkSql from PkDePara where Tabela = 'Pessoa' and PkIfx = ?),
            IdAtestado = (select PkSql from PkDePara where Tabela = 'AtestadoMedico' and PkIfx = ?),
            DataAtestado = ?,
            DataInclusao = ?,
            IdUsuarioValidacao = (select IdUsuario from Usuario where Matricula = ?),
            DataValidacao = ?,
            UltimaAlteracao = getdate()
        where
            IdPessoaAtestado = (select PkSql from PkDePara where Tabela = 'PessoaAtestadoMedico' and PkIfx = ?)
    """,(
            origem.cod_associado,
            origem.cod_atestado,
            origem.dat_atestado,
            origem.dat_inclusao,
            origem.mla_validador,
            origem.dat_validacao,
            linha_log.pk,
    ))

    if cr_sql.rowcount == 0:
        cr_sql.execute('begin transaction')

        cr_sql.execute(f"""
            insert into PessoaAtestadoMedico
            (
                IdPessoa,
                IdAtestado,
                DataAtestado,
                DataInclusao,
                IdUsuarioValidacao,
                DataValidacao
            ) values (
                (select PkSql from PkDePara where Tabela = 'Pessoa' and PkIfx = ?) /*IdPessoa*/,
                (select PkSql from PkDePara where Tabela = 'AtestadoMedico' and PkIfx = ?) /*IdAtestado*/,
                ? /*DataAtestado*/,
                ? /*DataInclusao*/,
                (select IdUsuario from Usuario where Matricula = ?) /*IdUsuarioValidacao*/,
                ? /*DataValidacao*/
            )
        """,(
            origem.cod_associado,
            origem.cod_atestado,
            origem.dat_atestado,
            origem.dat_inclusao,
            origem.mla_validador,
            origem.dat_validacao
        ))

        cr_sql.execute("""select ident_current('PessoaAtestadoMedico')""")
        pkSql = cr_sql.fetchval()
        IdPessoaAtestadoMedico = pkSql

        cr_sql.execute("insert into PkDePara values ('PessoaAtestadoMedico',?,?)",(pkSql, linha_log.pk,))
        cr_sql.execute("commit transaction")

    # Upload da imagem direto para o GCS (sem salvar em disco)

    if origem.img_atestado:
        blob = bucket.blob(f"PessoaAtestadoMedico/{IdPessoaAtestadoMedico}.jpg")
        blob.upload_from_string(
            origem.img_atestado,
            content_type='image/jpeg'
        )

    cr_sql.close()

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
            tabela = 'associado_atestado'
    """)
    Linha = recordtype('Linha',[col[0] for col in cr_ifx.description])

    for linha in [Linha(*l) for l in cr_ifx]:
        print(linha)
        try:
            convert(ifx, sql, linha)
        except Exception as erro:
            print(erro)
