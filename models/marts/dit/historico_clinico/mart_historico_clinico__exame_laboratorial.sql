{{
    config(
        schema="saude_historico_clinico",
        alias="exame_laboratorial",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}


with
    exame as (
        select * from {{ ref("int_historico_clinico__exame__cientificalab") }}
    ),

    selecao_exames as (
        select
            paciente_cpf,
            array_agg(
                struct(
                    id_cnes,
                    cod_apoio as codigo_do_exame,
                    data_assinatura as data_do_exame,
                    resultado,
                    unidade,
                    valor_referencia_minimo,
                    valor_referencia_maximo,
                    valor_referencia_texto
                )
            ) as exames,
            struct(datetime(current_timestamp(), 'America/Sao_Paulo') as processed_at) as metadados,
            safe_cast(paciente_cpf as int64) as cpf_particao
        from exame
        group by
            paciente_cpf
    )

select 
    paciente_cpf, 
    exames, 
    metadados, 
    cpf_particao
from selecao_exames
