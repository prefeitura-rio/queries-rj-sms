{{
    config(
        alias="indice",
        schema="app_historico_clinico",
        materialized="table",
        cluster_by="nome",
        partition_by={
            "field": "cns_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 1000000000000000, "interval": 333333333334},
        },
    )
}}

with
    source_paciente_mart as (
        select
            *
        from {{ ref('mart_historico_clinico__paciente') }}
    ),
    cns_mapeamento as (
        select
            valor_cns, cpf
        from source_paciente_mart, unnest(cns) as valor_cns
    ),


    source_paciente_app as (
        select
            *
        from {{ ref('mart_historico_clinico_app__paciente') }}
    ),

    -- -----------------------------------------
    -- Dados do paciente
    -- -----------------------------------------
    dados as (
        select
            cast(cns_mapeamento.valor_cns as int64) as cns_particao,
            source_paciente_app.cpf,
            registration_name as nome,
        from source_paciente_app
            inner join cns_mapeamento on cns_mapeamento.cpf = source_paciente_app.cpf
    )
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- FINAL
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from dados
where {{ validate_cpf("cpf") }}
