{{
    config(
        alias="saude_mental_ciclo_tratamento",
        schema="saude_historico_clinico",
        tags=["hci", "paciente", "saude_mental"],
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
        description="Ciclos de pacientes de caps da Prefeitura do Rio de Janeiro. Ciclo é um conjunto de atendimentos feitos em um ambulatório ou em um caps. Não se pode ter mais de um ciclo aberto ao mesmo tempo nem em um ambulatório nem em um CAPS."
    )
}}


with 
    ciclo_tratamento as (
        select 
            cpf,
            cns,
            ciclos_tratamento,
            struct(
                datetime(current_timestamp(), 'America/Sao_Paulo') as processed_at
            ) as metadados,
            cast(cpf as int64) as cpf_particao
        from {{ref('int_historico_clinico__ciclos_tratamento__pcsm')}}
        where cpf is not null
    )

select * from ciclo_tratamento  