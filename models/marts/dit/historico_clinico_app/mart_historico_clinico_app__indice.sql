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
    mapeamento as (
        select
            cpf,
            valor_cns
        from {{ ref("mart_historico_clinico__paciente") }},
            unnest(cns) as valor_cns
        where {{ validate_cpf("cpf") }}
    ),
    app as (
        select
            cpf,
            registration_name as nome
        from {{ ref("mart_historico_clinico_app__paciente") }}
    ),

    -- -----------------------------------------
    -- Dados do paciente
    -- -----------------------------------------
    dados as (
        select
            cast(mapeamento.valor_cns as int64) as cns_particao,
            app.cpf,
            app.nome,
        from app
        inner join mapeamento
            on mapeamento.cpf = app.cpf
    )
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- FINAL
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from dados
