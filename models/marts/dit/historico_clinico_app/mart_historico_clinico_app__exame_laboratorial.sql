{{
    config(
        alias="exame_laboratorial",
        schema="app_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with exame_laboratorial as (
    select
        *
    from {{ ref('mart_historico_clinico__exame_laboratorial') }}
),

    pacientes_com_hiv_reagente as (
        select distinct
            m.id_solicitacao
        from exame_laboratorial m
        left join {{ ref('raw_exames_laboratoriais__exames') }} e
            on m.id_solicitacao = e.id_solicitacao
        left join {{ ref('raw_exames_laboratoriais__resultados') }} r
            on e.id = r.id_exame
        where 
            e.codigo_lis = '0202030300'
            and lower(trim({{ remove_accents_upper("r.resultado") }})) in (
                'amostra reagente para hiv',
                'reagente',
                'amostra positiva para hiv'
            )
    ),

    paciente_restrito as (
        select 
            m.* except(id_solicitacao)
        from exame_laboratorial m
        left join pacientes_com_hiv_reagente h
            on m.id_solicitacao = h.id_solicitacao
        where h.id_solicitacao IS NULL  -- EXCLUI reagentes
    )

select *
from paciente_restrito

