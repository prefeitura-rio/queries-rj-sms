{{
    config(
        alias="exames_laboratoriais",
        schema="intermediario_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    solicitacoes as (
        select
            id,
            paciente_cpf,
            -- Remove "SMS RIO" do início do nome da unidade
            REGEXP_REPLACE(
                unidade,
                r"(?i)^SMS\s+(RIO)?\s*",
                ""
            ) as unidade_nome,
            laudo_url,
            datahora_pedido
        from {{ ref('raw_exames_laboratoriais__solicitacoes') }}
    ),

    exame as (
        select
            id_solicitacao as solicitacao_id,
            codigo_apoio,
            -- Conserta casos como o seguinte:
            -- "&lt;i&gt;ANDRE FILIPE PEREIRA DE OLIVEIRA SOARES (SMS CF MAESTRO CELES"
            -- 1) Remove '&lt;i&gt;' (<i> escapado)
            -- 2) Remove '(SMS ...' do final
            split(
                REGEXP_REPLACE(
                    solicitante_nome,
                    r"(?i)&lt;[a-z]*&gt;",
                    ""
                ),
                " (SMS"
            )[safe_offset(0)] as medico_solicitante,
            data_assinatura as datahora_assinatura
        from {{ ref('raw_exames_laboratoriais__exames') }}
    ),

    exames_com_resultados as (
        select
            s.id as id_solicitacao,
            s.paciente_cpf,
            -- Adiciona acentuação ao nome da unidade, capitalização adequada
            {{ proper_estabelecimento(add_accents_estabelecimento("s.unidade_nome")) }} as unidade_nome,
            e.codigo_apoio,
            s.laudo_url,
            {{ proper_br("e.medico_solicitante") }} as medico_solicitante,
            e.datahora_assinatura,
            s.datahora_pedido
        from solicitacoes as s
        inner join exame as e on s.id = e.solicitacao_id
        where laudo_url is not null
        and paciente_cpf is not null
    ),

    exame_deduplicado as (
        select *
        from exames_com_resultados
        qualify
            row_number() over (
                partition by paciente_cpf, codigo_apoio, datahora_assinatura
                order by datahora_assinatura desc
            ) = 1
    ),

    -- Descrições de códigos de exames a partir de planilha
    codigos_exames as (
        select
            {{ process_null("codigo") }} as codigo,
            {{ process_null("exame") }} as exame
        from {{ ref("raw_sheets__codigos_exames") }}
    ),

    exames_descritos as (
        select
            ex.*,
            cod.exame as exame_nome,
            safe_cast(paciente_cpf as int64) as cpf_particao
        from exame_deduplicado as ex
        left join codigos_exames as cod
            on ex.codigo_apoio = cod.codigo
    )

select * from exames_descritos
