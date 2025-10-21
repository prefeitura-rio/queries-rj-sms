{{
    config(
        alias="saude_mental_episodios",
        schema="app_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with sm_episodios as (
    select 
        * 
    from {{ ref('mart_historico_clinico_saude_mental') }}
    where {{validate_cpf('paciente_cpf')}}
)

select * from sm_episodios