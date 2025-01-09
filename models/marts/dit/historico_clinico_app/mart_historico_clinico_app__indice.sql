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
    source_mart as (
        select
            *
        from {{ ref('mart_historico_clinico__paciente') }}
    ),
    source_app as (
        select
            *
        from {{ ref('mart_historico_clinico_app__paciente') }}
    ),

    -- -----------------------------------------
    -- Enriquecimento
    -- -----------------------------------------
    cns_mapeamento as (
        select
            valor_cns, cpf
        from source_mart, unnest(cns) as valor_cns
    ),
    nome_mae_mapeamento as (
        select
            dados.mae_nome, cpf
        from source_mart
    ),

    -- -----------------------------------------
    -- Dados do paciente
    -- -----------------------------------------
    dados as (
        select
            cast(cns_mapeamento.valor_cns as int64) as cns_particao,
            source_app.cpf,
            registration_name as nome,
            birth_date as data_nascimento,
            {{ calculate_age('cast(birth_date as date)') }} AS idade,
            gender as genero,
            nome_mae_mapeamento.mae_nome as nome_mae,
            exibicao,

        from source_app
            inner join cns_mapeamento on cns_mapeamento.cpf = source_app.cpf
            inner join nome_mae_mapeamento on nome_mae_mapeamento.cpf = source_app.cpf
    )
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- FINAL
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from dados
where {{ validate_cpf("cpf") }}
