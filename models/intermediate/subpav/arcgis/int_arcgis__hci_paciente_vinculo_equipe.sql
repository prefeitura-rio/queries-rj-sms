{{
    config(
        alias = "arcgis__hci_paciente_vinculo_equipe",
        materialized = "table",
        tags = ["subpav", "arcgis", "hci", "onde_ser_atendido"]
    )
}}

with pacientes as (
    select
        cpf,
        dados.nome as nome,
        dados.data_nascimento as data_nascimento,
        equipe_saude_familia
    from {{ ref("mart_historico_clinico__paciente") }}
    where cpf is not null
),

vinculos as (
    select
        pacientes.cpf,
        pacientes.nome,
        pacientes.data_nascimento,

        lpad(
            regexp_replace(
                cast(esf.clinica_familia.id_cnes as string),
                r'\D',
                ''
            ),
            7,
            '0'
        ) as cnes_vinculo,

        cast(
            safe_cast(
                regexp_replace(cast(esf.id_ine as string), r'\D', '')
                as int64
            ) as string
        ) as ine_vinculo,

        cast(esf.nome as string) as nome_equipe_vinculo,
        cast(esf.clinica_familia.nome as string) as nome_unidade_vinculo,
        safe_cast(esf.datahora_ultima_atualizacao as timestamp)
            as datahora_ultima_atualizacao_vinculo,
        safe_cast(esf.rank as int64) as rank_vinculo

    from pacientes,
        unnest(pacientes.equipe_saude_familia) as esf
    where esf.id_ine is not null
        and trim(cast(esf.id_ine as string)) != ''
        and esf.clinica_familia.id_cnes is not null
),

deduplicado as (
    select *
    from vinculos
    qualify row_number() over (
        partition by cpf
        order by
            rank_vinculo asc,
            datahora_ultima_atualizacao_vinculo desc,
            cnes_vinculo,
            ine_vinculo
    ) = 1
)

select *
from deduplicado