{{
    config(
        alias="busca",
        schema="app_historico_clinico",
        materialized="table",
        cluster_by="nome",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
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
            cpf,
            array_agg(valor_cns) as cns_lista 
        from source_paciente_mart, unnest(cns) as valor_cns
        group by 1
    ),
    nome_mae_mapeamento as (
        select
            cpf,
            dados.mae_nome
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
            cast(source_paciente_app.cpf as int64) as cpf_particao,

            source_paciente_app.cpf,
            cns_lista,

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
