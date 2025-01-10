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
    source_paciente_app as (
        select
            *
        from {{ ref('mart_historico_clinico_app__paciente') }}
    ),
    source_episodio_app as (
        select
            *
        from {{ ref('mart_historico_clinico_app__episodio') }}
    ),

    -- -----------------------------------------
    -- Enriquecimento
    -- -----------------------------------------
    cns_mapeamento as (
        select
            valor_cns, cpf
        from source_paciente_mart, unnest(cns) as valor_cns
    ),
    nome_mae_mapeamento as (
        select
            dados.mae_nome, cpf
        from source_paciente_mart
    ),
    episodios_por_pessoas as (
        select
            source_episodio_app.cpf,
            count(*) as quantidade_episodios
        from source_episodio_app
        group by 1
    ),

    -- -----------------------------------------
    -- Dados do paciente
    -- -----------------------------------------
    dados as (
        select
            cast(cns_mapeamento.valor_cns as int64) as cns_particao,
            source_paciente_app.cpf,
            registration_name as nome,
            birth_date as data_nascimento,
            {{ calculate_age('cast(birth_date as date)') }} AS idade,
            gender as genero,
            nome_mae_mapeamento.mae_nome as nome_mae,
            coalesce(quantidade_episodios, 0) as quantidade_episodios,
            exibicao
        from source_paciente_app
            inner join cns_mapeamento on cns_mapeamento.cpf = source_paciente_app.cpf
            inner join nome_mae_mapeamento on nome_mae_mapeamento.cpf = source_paciente_app.cpf
            left join episodios_por_pessoas on episodios_por_pessoas.cpf = source_paciente_app.cpf
    )
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- FINAL
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from dados
where {{ validate_cpf("cpf") }}
