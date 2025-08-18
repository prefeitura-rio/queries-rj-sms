{{
    config(
        schema="saude_historico_clinico",
        alias="vacinacao",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}


with
    vacinas as (
        select * from {{ ref("int_historico_clinico__vacinacao__vitacare") }}
    ),

    vacinas_deduplicados as (
        select *,
        from vacinas
        qualify
            row_number() over (
                partition by cpf,nome_vacina,dose,aplicacao_data order by registro_data desc
            ) = 1
    ),

    selecao_vacinas as (
        select
            cpf,
            array_agg(
                struct(
                    id,
                    id_cnes,
                    nome_vacina,
                    dose,
                    aplicacao_data,
                    registro_data,
                    diff,
                    lote,
                    registro_tipo,
                    estrategia_imunizacao
                )
            ) as vacinacao,
            struct(datetime(current_timestamp(), 'America/Sao_Paulo') as processed_at) as metadados,
            safe_cast(cpf as int64) as cpf_particao
        from vacinas_deduplicados
        group by
            cpf
    )

select 
    cpf, 
    vacinacao, 
    metadados, 
    cpf_particao
from selecao_vacinas
