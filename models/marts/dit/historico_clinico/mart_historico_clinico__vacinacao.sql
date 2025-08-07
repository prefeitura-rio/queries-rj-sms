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
        select * from {{ ref("int_historico_clinico__vacinacao__api") }}
        union all
        select * from {{ ref('int_historico_clinico__vacinacao__historico') }}
        union all
        select * from {{ ref('int_historico_clinico__vacinacao__continuo') }} 
    ),

    vacinas_deduplicados as (
        select *,
        from vacinas
        qualify
            row_number() over (
                partition by cpf,nome_vacina,dose,data_aplicacao order by data_registro desc
            ) = 1
    ),

    vacinacao as (
        select
            cpf,
            nome,
            data_nascimento,
            nome_mae as mae_nome,
            array_agg(
                struct(
                    nome_vacina,
                    dose,
                    data_aplicacao,
                    data_registro,
                    cnes_unidade
                )
            ) as vacinacoes,
            current_timestamp() at time zone 'America/Sao_Paulo' as processed_at,
            safe_cast(cpf as int64) as cpf_particao
        from vacinas_deduplicados
        group by
            cpf,
            nome,
            data_nascimento,
            mae_nome
    )

select 
    cpf, 
    nome,
    data_nascimento,
    mae_nome,
    vacinacoes, 
    processed_at, 
    cpf_particao
from vacinacao