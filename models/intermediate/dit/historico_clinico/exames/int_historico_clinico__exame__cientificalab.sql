{{
    config(
        schema="intermediario_historico_clinico",
        alias="exames_cientificalab",
        materialized="table",
    )
}}

with
    solicitacoes as (
        select
            id,
            paciente_cpf,
            -- Remove "SMS RIO" do início do nome da unidade
            REGEXP_REPLACE(
                origem,
                r"(?i)^SMS\s+(RIO)?\s*",
                ""
            ) as unidade_nome,
            laudo_url
        from {{ ref('raw_cientificalab__solicitacoes') }}
    ),

    exame as (
        select
            solicitacao_id,
            cod_apoio as codigo_apoio,
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
            SAFE.PARSE_DATETIME("%d/%m/%Y %H:%M:%S", data_assinatura) as data_assinatura
        from {{ ref('raw_cientificalab__exames') }}
    ),

    exames_com_resultados as (
        select
            s.paciente_cpf,
            -- Adiciona acentuação ao nome da unidade, capitalização adequada
            {{ proper_estabelecimento(add_accents_estabelecimento("s.unidade_nome")) }} as unidade_nome,
            e.codigo_apoio,
            s.laudo_url,
            {{ proper_br("e.medico_solicitante") }} as medico_solicitante,
            e.data_assinatura,
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
                partition by paciente_cpf, codigo_apoio, data_assinatura order by data_assinatura desc
            ) = 1
    ),

    codigos_exames as (
        select codigo, exame
        from {{ ref("raw_sheets__codigos_exames") }}
    ),

    exames_descritos as (
        select
            ex.*,
            cod.exame as exame_nome,
        from exame_deduplicado as ex
        left join codigos_exames as cod
            on ex.codigo_apoio = cod.codigo
    )

select * from exames_descritos
