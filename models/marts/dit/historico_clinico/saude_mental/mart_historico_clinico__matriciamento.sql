{{
    config(
        alias="saude_mental_matriciamento",
        schema="saude_historico_clinico",
        tags=["hci", "paciente", "saude_mental"],
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
        description="Matriciamentos feitos em unidades de saúde psico-sociais da Prefeitura do Rio de Janeiro. Matriciamento é uma estratégia de organização do cuidado em saúde mental baseada na interdisciplinaridade e na articulação em rede."
        
    )
}}


with 
    matriciamento as (
        select 
            cpf,
            cns,
            matriciamentos,
            struct(
                datetime(current_timestamp(), 'America/Sao_Paulo') as processed_at
            ) as metadados,
            cast(cpf as int64) as cpf_particao
        from {{ref('int_historico_clinico__matriciamento__pcsm')}}
        where cpf is not null
    )

select * from matriciamento