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
    source as (
        select
            *
        from {{ ref('mart_historico_clinico__paciente') }}
    ),
    dados_paciente as (
        select
            cast(valor_cns as int64) as cns_particao,
            cpf,
            source.dados.nome,
            valor_cns,
            source.dados.data_nascimento,
            {{ calculate_age('source.dados.data_nascimento') }} AS idade,
            source.dados.genero,
            source.dados.mae_nome
        from source, unnest(cns) as valor_cns
    )
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- FINAL
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from dados_paciente
where {{ validate_cpf("cpf") }}
