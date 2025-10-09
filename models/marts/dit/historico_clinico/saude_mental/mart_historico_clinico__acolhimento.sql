{{
    config(
        alias="saude_mental_acolhimento",
        schema="saude_historico_clinico",
        tags=["hci", "paciente", "saude_mental"],
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
        description= "Acolhimentos feitos em unidades de acolhimento (tipos especiais de unidades de saúde) da Prefeitura do Rio de Janeiro. Acolhimento é a recepção temporária para o cuidado de pacientes de saúde mental. Um acolhimento é período de uso de um leito."
    )
}}

with 
    acolhimentos as (
        select 
            cpf,
            cns,
            acolhimentos,
            struct(
                datetime(current_timestamp(), 'America/Sao_Paulo') as processed_at
            ) as metadados,
            cast(cpf as int64) as cpf_particao
        from {{ref('int_historico_clinico__acolhimentos__pcsm')}}
        where cpf is not null
    )

select * from acolhimentos